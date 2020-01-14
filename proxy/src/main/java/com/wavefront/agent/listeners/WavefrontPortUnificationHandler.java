package com.wavefront.agent.listeners;

import com.google.common.collect.Lists;

import com.wavefront.common.Utils;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.channel.SharedGraphiteHostAnnotator;
import com.wavefront.agent.formatter.DataFormat;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.SourceTag;
import com.wavefront.ingester.ReportableEntityDecoder;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import wavefront.report.ReportEvent;
import wavefront.report.ReportPoint;
import wavefront.report.ReportSourceTag;

import static com.wavefront.agent.channel.ChannelUtils.formatErrorMessage;

/**
 * Process incoming Wavefront-formatted data. Also allows sourceTag formatted data and
 * histogram-formatted data pass-through with lazy-initialized handlers.
 *
 * Accepts incoming messages of either String or FullHttpRequest type: single data point in a
 * string, or multiple points in the HTTP post body, newline-delimited.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class WavefrontPortUnificationHandler extends AbstractLineDelimitedHandler {

  @Nullable
  private final SharedGraphiteHostAnnotator annotator;
  @Nullable
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final ReportableEntityDecoder<String, ReportPoint> wavefrontDecoder;
  private final ReportableEntityDecoder<String, ReportSourceTag> sourceTagDecoder;
  private final ReportableEntityDecoder<String, ReportEvent> eventDecoder;
  private final ReportableEntityDecoder<String, ReportPoint> histogramDecoder;
  private final ReportableEntityHandler<ReportPoint, String> wavefrontHandler;
  private final Supplier<ReportableEntityHandler<ReportPoint, String>> histogramHandlerSupplier;
  private final Supplier<ReportableEntityHandler<ReportSourceTag, SourceTag>> sourceTagHandlerSupplier;
  private final Supplier<ReportableEntityHandler<ReportEvent, ReportEvent>> eventHandlerSupplier;

  /**
   * Create new instance with lazy initialization for handlers.
   *
   * @param handle              handle/port number.
   * @param tokenAuthenticator  tokenAuthenticator for incoming requests.
   * @param healthCheckManager  shared health check endpoint handler.
   * @param decoders            decoders.
   * @param handlerFactory      factory for ReportableEntityHandler objects.
   * @param annotator           hostAnnotator that makes sure all points have a source= tag.
   * @param preprocessor        preprocessor.
   */
  @SuppressWarnings("unchecked")
  public WavefrontPortUnificationHandler(
      final String handle, final TokenAuthenticator tokenAuthenticator,
      final HealthCheckManager healthCheckManager,
      final Map<ReportableEntityType, ReportableEntityDecoder<?, ?>> decoders,
      final ReportableEntityHandlerFactory handlerFactory,
      @Nullable final SharedGraphiteHostAnnotator annotator,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor) {
    super(tokenAuthenticator, healthCheckManager, handle);
    this.wavefrontDecoder = (ReportableEntityDecoder<String, ReportPoint>) decoders.
        get(ReportableEntityType.POINT);
    this.annotator = annotator;
    this.preprocessorSupplier = preprocessor;
    this.wavefrontHandler = handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT, handle));
    this.histogramDecoder = (ReportableEntityDecoder<String, ReportPoint>) decoders.
        get(ReportableEntityType.HISTOGRAM);
    this.sourceTagDecoder = (ReportableEntityDecoder<String, ReportSourceTag>) decoders.
        get(ReportableEntityType.SOURCE_TAG);
    this.eventDecoder = (ReportableEntityDecoder<String, ReportEvent>) decoders.
        get(ReportableEntityType.EVENT);
    this.histogramHandlerSupplier = Utils.lazySupplier(() -> handlerFactory.getHandler(
        HandlerKey.of(ReportableEntityType.HISTOGRAM, handle)));
    this.sourceTagHandlerSupplier = Utils.lazySupplier(() -> handlerFactory.getHandler(
        HandlerKey.of(ReportableEntityType.SOURCE_TAG, handle)));
    this.eventHandlerSupplier = Utils.lazySupplier(() -> handlerFactory.getHandler(
        HandlerKey.of(ReportableEntityType.EVENT, handle)));
  }

  /**
   *
   * @param ctx      ChannelHandler context (to retrieve remote client's IP in case of errors)
   * @param message  line being processed
   */
  @Override
  protected void processLine(final ChannelHandlerContext ctx, String message) {
    if (message.isEmpty()) return;
    DataFormat dataFormat = DataFormat.autodetect(message);
    switch (dataFormat) {
      case SOURCE_TAG:
        ReportableEntityHandler<ReportSourceTag, SourceTag> sourceTagHandler =
            sourceTagHandlerSupplier.get();
        if (sourceTagHandler == null || sourceTagDecoder == null) {
          wavefrontHandler.reject(message, "Port is not configured to accept " +
              "sourceTag-formatted data!");
          return;
        }
        List<ReportSourceTag> output = Lists.newArrayListWithCapacity(1);
        try {
          sourceTagDecoder.decode(message, output, "dummy");
          for (ReportSourceTag tag : output) {
            sourceTagHandler.report(tag);
          }
        } catch (Exception e) {
          sourceTagHandler.reject(message, formatErrorMessage("WF-300 Cannot parse: \"" + message +
              "\"", e, ctx));
        }
        return;
      case EVENT:
        ReportableEntityHandler<ReportEvent, ReportEvent> eventHandler = eventHandlerSupplier.get();
        if (eventHandler == null || eventDecoder == null) {
          wavefrontHandler.reject(message, "Port is not configured to accept event data!");
          return;
        }
        List<ReportEvent> events = Lists.newArrayListWithCapacity(1);
        try {
          eventDecoder.decode(message, events, "dummy");
          for (ReportEvent event : events) {
            eventHandler.report(event);
          }
        } catch (Exception e) {
          eventHandler.reject(message, formatErrorMessage("WF-300 Cannot parse: \"" + message +
              "\"", e, ctx));
        }
        return;
      case HISTOGRAM:
        ReportableEntityHandler<ReportPoint, String> histogramHandler = histogramHandlerSupplier.get();
        if (histogramHandler == null || histogramDecoder == null) {
          wavefrontHandler.reject(message, "Port is not configured to accept " +
              "histogram-formatted data!");
          return;
        }
        message = annotator == null ? message : annotator.apply(ctx, message);
        preprocessAndHandlePoint(message, histogramDecoder, histogramHandler, preprocessorSupplier,
            ctx);
        return;
      default:
        message = annotator == null ? message : annotator.apply(ctx, message);
        preprocessAndHandlePoint(message, wavefrontDecoder, wavefrontHandler, preprocessorSupplier,
            ctx);
    }
  }

  static void preprocessAndHandlePoint(
      String message, ReportableEntityDecoder<String, ReportPoint> decoder,
      ReportableEntityHandler<ReportPoint, String> handler,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      @Nullable ChannelHandlerContext ctx) {
    ReportableEntityPreprocessor preprocessor = preprocessorSupplier == null ?
        null : preprocessorSupplier.get();
    String[] messageHolder = new String[1];
    // transform the line if needed
    if (preprocessor != null) {
      message = preprocessor.forPointLine().transform(message);

      // apply white/black lists after formatting
      if (!preprocessor.forPointLine().filter(message, messageHolder)) {
        if (messageHolder[0] != null) {
          handler.reject((ReportPoint) null, message);
        } else {
          handler.block(null, message);
        }
        return;
      }
    }

    List<ReportPoint> output = Lists.newArrayListWithCapacity(1);
    try {
      decoder.decode(message, output, "dummy");
    } catch (Exception e) {
      handler.reject(message, formatErrorMessage("WF-300 Cannot parse: \"" + message + "\"", e,
          ctx));
      return;
    }

    for (ReportPoint object : output) {
      if (preprocessor != null) {
        preprocessor.forReportPoint().transform(object);
        if (!preprocessor.forReportPoint().filter(object, messageHolder)) {
          if (messageHolder[0] != null) {
            handler.reject(object, messageHolder[0]);
          } else {
            handler.block(object);
          }
          return;
        }
      }
      handler.report(object);
    }
  }
}
