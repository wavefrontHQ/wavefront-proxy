package com.wavefront.agent.listeners;

import com.google.common.collect.Lists;

import com.wavefront.agent.channel.CachingGraphiteHostAnnotator;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportSourceTagDecoder;
import com.wavefront.ingester.ReportableEntityDecoder;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import wavefront.report.ReportPoint;
import wavefront.report.ReportSourceTag;

/**
 * Process incoming Wavefront-formatted data. Also allows sourceTag formatted data and histogram-formatted data
 * pass-through with lazy-initialized handlers.
 *
 * Accepts incoming messages of either String or FullHttpRequest type: single data point in a string,
 * or multiple points in the HTTP post body, newline-delimited.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class WavefrontPortUnificationHandler extends PortUnificationHandler {
  private static final Logger logger = Logger.getLogger(WavefrontPortUnificationHandler.class.getCanonicalName());

  @Nullable
  private final CachingGraphiteHostAnnotator annotator;

  @Nullable
  private final ReportableEntityPreprocessor preprocessor;

  private final ReportableEntityHandlerFactory handlerFactory;
  private final Map<ReportableEntityType, ReportableEntityDecoder> decoders;

  private final ReportableEntityDecoder<String, ReportPoint> wavefrontDecoder;
  private volatile ReportableEntityDecoder<String, ReportSourceTag> sourceTagDecoder;
  private volatile ReportableEntityDecoder<String, ReportPoint> histogramDecoder;
  private final ReportableEntityHandler<ReportPoint> wavefrontHandler;
  private volatile ReportableEntityHandler<ReportSourceTag> sourceTagHandler;
  private volatile ReportableEntityHandler<ReportPoint> histogramHandler;

  /**
   * Create new instance with lazy initialization for handlers.
   *
   * @param handle         handle/port number.
   * @param decoders       decoders.
   * @param handlerFactory factory for ReportableEntityHandler objects.
   * @param annotator      hostAnnotator that makes sure all points have a source= tag.
   * @param preprocessor   preprocessor.
   */
  @SuppressWarnings("unchecked")
  public WavefrontPortUnificationHandler(final String handle,
                                         final Map<ReportableEntityType, ReportableEntityDecoder> decoders,
                                         final ReportableEntityHandlerFactory handlerFactory,
                                         @Nullable final CachingGraphiteHostAnnotator annotator,
                                         @Nullable final ReportableEntityPreprocessor preprocessor) {
    super(handle);
    this.decoders = decoders;
    this.wavefrontDecoder = (ReportableEntityDecoder<String, ReportPoint>)(decoders.get(ReportableEntityType.POINT));
    this.handlerFactory = handlerFactory;
    this.annotator = annotator;
    this.preprocessor = preprocessor;
    this.wavefrontHandler = handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT, handle));
  }

  /**
   *
   * @param ctx      ChannelHandler context (to retrieve remote client's IP in case of errors)
   * @param message  line being processed
   */
  @Override
  @SuppressWarnings("unchecked")
  protected void processLine(final ChannelHandlerContext ctx, String message) {
    if (message.isEmpty()) return;

    if (message.startsWith(ReportSourceTagDecoder.SOURCE_TAG) ||
        message.startsWith(ReportSourceTagDecoder.SOURCE_DESCRIPTION)) {
      if (sourceTagHandler == null) {
        synchronized(this) {
          if (sourceTagHandler == null && handlerFactory != null && decoders != null) {
            sourceTagHandler = handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.SOURCE_TAG, handle));
            sourceTagDecoder = decoders.get(ReportableEntityType.SOURCE_TAG);
          }
          if (sourceTagHandler == null || sourceTagDecoder == null) {
            wavefrontHandler.reject(message, "Port is not configured to accept sourceTag-formatted data!");
            return;
          }
        }
      }
      List<ReportSourceTag> output = Lists.newArrayListWithCapacity(1);
      try {
        sourceTagDecoder.decode(message, output, "dummy");
        for(ReportSourceTag tag : output) {
          sourceTagHandler.report(tag);
        }
      } catch (Exception e) {
        sourceTagHandler.reject(message, formatErrorMessage("WF-300 Cannot parse: \"" + message + "\"", e, ctx));
      }
      return;
    }

    ReportableEntityHandler<ReportPoint> handler;
    ReportableEntityDecoder<String, ReportPoint> decoder;

    if (message.startsWith("!M ") || message.startsWith("!H ") || message.startsWith("!D ")) {
      if (histogramHandler == null) {
        synchronized(this) {
          if (histogramHandler == null && handlerFactory != null && decoders != null) {
            histogramHandler = handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.HISTOGRAM,
                handle + "-histograms"));
            histogramDecoder = decoders.get(ReportableEntityType.HISTOGRAM);
          }
          if (histogramHandler == null || histogramDecoder == null) {
            wavefrontHandler.reject(message, "Port is not configured to accept histogram-formatted data!");
            return;
          }
        }
      }
      handler = histogramHandler;
      decoder = histogramDecoder;
      message = annotator == null ? message : annotator.apply(ctx, message);
    } else {
      handler = wavefrontHandler;
      decoder = wavefrontDecoder;
      message = annotator == null ? message : annotator.apply(ctx, message);
    }

    // transform the line if needed
    if (preprocessor != null) {
      message = preprocessor.forPointLine().transform(message);

      // apply white/black lists after formatting
      if (!preprocessor.forPointLine().filter(message)) {
        if (preprocessor.forPointLine().getLastFilterResult() != null) {
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
      handler.reject(message, formatErrorMessage("WF-300 Cannot parse: \"" + message + "\"", e, ctx));
      return;
    }

    for (ReportPoint object : output) {
      if (preprocessor != null) {
        preprocessor.forReportPoint().transform(object);
        if (!preprocessor.forReportPoint().filter(object)) {
          if (preprocessor.forReportPoint().getLastFilterResult() != null) {
            handler.reject(object, preprocessor.forReportPoint().getLastFilterResult());
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
