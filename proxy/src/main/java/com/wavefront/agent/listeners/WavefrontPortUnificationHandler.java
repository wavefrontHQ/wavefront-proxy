package com.wavefront.agent.listeners;

import static com.wavefront.agent.LogsUtil.LOGS_DATA_FORMATS;
import static com.wavefront.agent.channel.ChannelUtils.formatErrorMessage;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static com.wavefront.agent.formatter.DataFormat.*;
import static com.wavefront.agent.listeners.FeatureCheckUtils.HISTO_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.LOGS_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.SPANLOGS_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.SPAN_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.isFeatureDisabled;
import static com.wavefront.agent.listeners.tracing.SpanUtils.handleSpanLogs;
import static com.wavefront.agent.listeners.tracing.SpanUtils.preprocessAndHandleSpan;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.channel.SharedGraphiteHostAnnotator;
import com.wavefront.agent.formatter.DataFormat;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.common.Utils;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.SourceTag;
import com.wavefront.ingester.ReportableEntityDecoder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import wavefront.report.ReportEvent;
import wavefront.report.ReportLog;
import wavefront.report.ReportPoint;
import wavefront.report.ReportSourceTag;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

/**
 * Process incoming Wavefront-formatted data. Also allows sourceTag formatted data and
 * histogram-formatted data pass-through with lazy-initialized handlers.
 *
 * <p>Accepts incoming messages of either String or FullHttpRequest type: single data point in a
 * string, or multiple points in the HTTP post body, newline-delimited.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class WavefrontPortUnificationHandler extends AbstractLineDelimitedHandler {
  @Nullable private final SharedGraphiteHostAnnotator annotator;
  @Nullable private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final ReportableEntityDecoder<String, ReportPoint> wavefrontDecoder;
  private final ReportableEntityDecoder<String, ReportSourceTag> sourceTagDecoder;
  private final ReportableEntityDecoder<String, ReportEvent> eventDecoder;
  private final ReportableEntityDecoder<String, ReportPoint> histogramDecoder;
  private final ReportableEntityDecoder<String, Span> spanDecoder;
  private final ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDecoder;
  private final ReportableEntityDecoder<String, ReportLog> logDecoder;
  private final ReportableEntityHandler<ReportPoint, String> wavefrontHandler;
  private final Supplier<ReportableEntityHandler<ReportPoint, String>> histogramHandlerSupplier;
  private final Supplier<ReportableEntityHandler<ReportSourceTag, SourceTag>>
      sourceTagHandlerSupplier;
  private final Supplier<ReportableEntityHandler<Span, String>> spanHandlerSupplier;
  private final Supplier<ReportableEntityHandler<SpanLogs, String>> spanLogsHandlerSupplier;
  private final Supplier<ReportableEntityHandler<ReportEvent, ReportEvent>> eventHandlerSupplier;
  private final Supplier<ReportableEntityHandler<ReportLog, ReportLog>> logHandlerSupplier;

  private final Supplier<Boolean> histogramDisabled;
  private final Supplier<Boolean> traceDisabled;
  private final Supplier<Boolean> spanLogsDisabled;
  private final Supplier<Boolean> logsDisabled;

  private final SpanSampler sampler;

  private final Supplier<Counter> receivedSpansTotal;
  private final Supplier<Counter> discardedHistograms;
  private final Supplier<Counter> discardedSpans;
  private final Supplier<Counter> discardedSpanLogs;
  private final Supplier<Counter> discardedSpansBySampler;
  private final Supplier<Counter> discardedSpanLogsBySampler;
  private final LoadingCache<DataFormat, Counter> receivedLogsCounter;
  private final LoadingCache<DataFormat, Counter> discardedLogsCounter;

  /**
   * Create new instance with lazy initialization for handlers.
   *
   * @param handle handle/port number.
   * @param tokenAuthenticator tokenAuthenticator for incoming requests.
   * @param healthCheckManager shared health check endpoint handler.
   * @param decoders decoders.
   * @param handlerFactory factory for ReportableEntityHandler objects.
   * @param annotator hostAnnotator that makes sure all points have a source= tag.
   * @param preprocessor preprocessor supplier.
   * @param histogramDisabled supplier for backend-controlled feature flag for histograms.
   * @param traceDisabled supplier for backend-controlled feature flag for spans.
   * @param spanLogsDisabled supplier for backend-controlled feature flag for span logs.
   * @param sampler handles sampling of spans and span logs.
   * @param logsDisabled supplier for backend-controlled feature flag for logs.
   */
  @SuppressWarnings("unchecked")
  public WavefrontPortUnificationHandler(
      final String handle,
      final TokenAuthenticator tokenAuthenticator,
      final HealthCheckManager healthCheckManager,
      final Map<ReportableEntityType, ReportableEntityDecoder<?, ?>> decoders,
      final ReportableEntityHandlerFactory handlerFactory,
      @Nullable final SharedGraphiteHostAnnotator annotator,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor,
      final Supplier<Boolean> histogramDisabled,
      final Supplier<Boolean> traceDisabled,
      final Supplier<Boolean> spanLogsDisabled,
      final SpanSampler sampler,
      final Supplier<Boolean> logsDisabled) {
    super(tokenAuthenticator, healthCheckManager, handle);
    this.wavefrontDecoder =
        (ReportableEntityDecoder<String, ReportPoint>) decoders.get(ReportableEntityType.POINT);
    this.annotator = annotator;
    this.preprocessorSupplier = preprocessor;
    this.wavefrontHandler =
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT, handle));
    this.histogramDecoder =
        (ReportableEntityDecoder<String, ReportPoint>) decoders.get(ReportableEntityType.HISTOGRAM);
    this.sourceTagDecoder =
        (ReportableEntityDecoder<String, ReportSourceTag>)
            decoders.get(ReportableEntityType.SOURCE_TAG);
    this.spanDecoder =
        (ReportableEntityDecoder<String, Span>) decoders.get(ReportableEntityType.TRACE);
    this.spanLogsDecoder =
        (ReportableEntityDecoder<JsonNode, SpanLogs>)
            decoders.get(ReportableEntityType.TRACE_SPAN_LOGS);
    this.eventDecoder =
        (ReportableEntityDecoder<String, ReportEvent>) decoders.get(ReportableEntityType.EVENT);
    this.logDecoder =
        (ReportableEntityDecoder<String, ReportLog>) decoders.get(ReportableEntityType.LOGS);
    this.histogramHandlerSupplier =
        Utils.lazySupplier(
            () -> handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.HISTOGRAM, handle)));
    this.sourceTagHandlerSupplier =
        Utils.lazySupplier(
            () ->
                handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.SOURCE_TAG, handle)));
    this.spanHandlerSupplier =
        Utils.lazySupplier(
            () -> handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)));
    this.spanLogsHandlerSupplier =
        Utils.lazySupplier(
            () ->
                handlerFactory.getHandler(
                    HandlerKey.of(ReportableEntityType.TRACE_SPAN_LOGS, handle)));
    this.eventHandlerSupplier =
        Utils.lazySupplier(
            () -> handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.EVENT, handle)));
    this.logHandlerSupplier =
        Utils.lazySupplier(
            () -> handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.LOGS, handle)));
    this.histogramDisabled = histogramDisabled;
    this.traceDisabled = traceDisabled;
    this.spanLogsDisabled = spanLogsDisabled;
    this.logsDisabled = logsDisabled;
    this.sampler = sampler;
    this.discardedHistograms =
        Utils.lazySupplier(
            () -> Metrics.newCounter(new MetricName("histogram", "", "discarded_points")));
    this.discardedSpans =
        Utils.lazySupplier(
            () -> Metrics.newCounter(new MetricName("spans." + handle, "", "discarded")));
    this.discardedSpanLogs =
        Utils.lazySupplier(
            () -> Metrics.newCounter(new MetricName("spanLogs." + handle, "", "discarded")));
    this.discardedSpansBySampler =
        Utils.lazySupplier(
            () -> Metrics.newCounter(new MetricName("spans." + handle, "", "sampler.discarded")));
    this.discardedSpanLogsBySampler =
        Utils.lazySupplier(
            () ->
                Metrics.newCounter(new MetricName("spanLogs." + handle, "", "sampler.discarded")));
    this.receivedSpansTotal =
        Utils.lazySupplier(
            () -> Metrics.newCounter(new MetricName("spans." + handle, "", "received.total")));
    this.receivedLogsCounter =
        Caffeine.newBuilder()
            .build(
                format ->
                    Metrics.newCounter(
                        new TaggedMetricName(
                            "logs." + handle,
                            "received" + ".total",
                            "format",
                            format.name().toLowerCase())));
    this.discardedLogsCounter =
        Caffeine.newBuilder()
            .build(
                format ->
                    Metrics.newCounter(
                        new TaggedMetricName(
                            "logs." + handle, "discarded", "format", format.name().toLowerCase())));
  }

  @Override
  protected DataFormat getFormat(FullHttpRequest httpRequest) {
    return DataFormat.parse(
        URLEncodedUtils.parse(URI.create(httpRequest.uri()), CharsetUtil.UTF_8).stream()
            .filter(x -> x.getName().equals("format") || x.getName().equals("f"))
            .map(NameValuePair::getValue)
            .findFirst()
            .orElse(null));
  }

  @Override
  protected void handleHttpMessage(ChannelHandlerContext ctx, FullHttpRequest request) {
    StringBuilder out = new StringBuilder();
    DataFormat format = getFormat(request);
    if ((format == HISTOGRAM
            && isFeatureDisabled(
                histogramDisabled, HISTO_DISABLED, discardedHistograms.get(), out, request))
        || (format == SPAN_LOG
            && isFeatureDisabled(
                spanLogsDisabled, SPANLOGS_DISABLED, discardedSpanLogs.get(), out, request))) {
      writeHttpResponse(ctx, HttpResponseStatus.FORBIDDEN, out, request);
      return;
    } else if (format == SPAN
        && isFeatureDisabled(traceDisabled, SPAN_DISABLED, discardedSpans.get(), out, request)) {
      receivedSpansTotal.get().inc(discardedSpans.get().count());
      writeHttpResponse(ctx, HttpResponseStatus.FORBIDDEN, out, request);
      return;
    } else if ((LOGS_DATA_FORMATS.contains(format))
        && isFeatureDisabled(
            logsDisabled, LOGS_DISABLED, discardedLogsCounter.get(format), out, request)) {
      receivedLogsCounter.get(format).inc(discardedLogsCounter.get(format).count());
      writeHttpResponse(ctx, HttpResponseStatus.FORBIDDEN, out, request);
      return;
    }
    super.handleHttpMessage(ctx, request);
  }

  /**
   * @param ctx ChannelHandler context (to retrieve remote client's IP in case of errors)
   * @param message line being processed
   */
  @Override
  protected void processLine(
      final ChannelHandlerContext ctx, @Nonnull String message, @Nullable DataFormat format) {

    if (message.contains("\04")) {
      wavefrontHandler.reject(message, "'EOT' character is not allowed!");
    }

    DataFormat dataFormat = format == null ? DataFormat.autodetect(message) : format;
    switch (dataFormat) {
      case SOURCE_TAG:
        ReportableEntityHandler<ReportSourceTag, SourceTag> sourceTagHandler =
            sourceTagHandlerSupplier.get();
        if (sourceTagHandler == null || sourceTagDecoder == null) {
          wavefrontHandler.reject(
              message, "Port is not configured to accept " + "sourceTag-formatted data!");
          return;
        }
        List<ReportSourceTag> output = new ArrayList<>(1);
        try {
          sourceTagDecoder.decode(message, output, "dummy");
          for (ReportSourceTag tag : output) {
            sourceTagHandler.report(tag);
          }
        } catch (Exception e) {
          sourceTagHandler.reject(
              message,
              formatErrorMessage("WF-300 Cannot parse sourceTag: \"" + message + "\"", e, ctx));
        }
        return;
      case EVENT:
        ReportableEntityHandler<ReportEvent, ReportEvent> eventHandler = eventHandlerSupplier.get();
        if (eventHandler == null || eventDecoder == null) {
          wavefrontHandler.reject(message, "Port is not configured to accept event data!");
          return;
        }
        List<ReportEvent> events = new ArrayList<>(1);
        try {
          eventDecoder.decode(message, events, "dummy");
          for (ReportEvent event : events) {
            eventHandler.report(event);
          }
        } catch (Exception e) {
          eventHandler.reject(
              message,
              formatErrorMessage("WF-300 Cannot parse event: \"" + message + "\"", e, ctx));
        }
        return;
      case SPAN:
        ReportableEntityHandler<Span, String> spanHandler = spanHandlerSupplier.get();
        if (spanHandler == null || spanDecoder == null) {
          wavefrontHandler.reject(
              message, "Port is not configured to accept " + "tracing data (spans)!");
          return;
        }
        message = annotator == null ? message : annotator.apply(ctx, message);
        receivedSpansTotal.get().inc();
        preprocessAndHandleSpan(
            message,
            spanDecoder,
            spanHandler,
            spanHandler::report,
            preprocessorSupplier,
            ctx,
            span -> sampler.sample(span, discardedSpansBySampler.get()));
        return;
      case SPAN_LOG:
        if (isFeatureDisabled(spanLogsDisabled, SPANLOGS_DISABLED, discardedSpanLogs.get())) return;
        ReportableEntityHandler<SpanLogs, String> spanLogsHandler = spanLogsHandlerSupplier.get();
        if (spanLogsHandler == null || spanLogsDecoder == null || spanDecoder == null) {
          wavefrontHandler.reject(
              message, "Port is not configured to accept " + "tracing data (span logs)!");
          return;
        }
        handleSpanLogs(
            message,
            spanLogsDecoder,
            spanDecoder,
            spanLogsHandler,
            preprocessorSupplier,
            ctx,
            span -> sampler.sample(span, discardedSpanLogsBySampler.get()));
        return;
      case HISTOGRAM:
        if (isFeatureDisabled(histogramDisabled, HISTO_DISABLED, discardedHistograms.get())) return;
        ReportableEntityHandler<ReportPoint, String> histogramHandler =
            histogramHandlerSupplier.get();
        if (histogramHandler == null || histogramDecoder == null) {
          wavefrontHandler.reject(
              message, "Port is not configured to accept " + "histogram-formatted data!");
          return;
        }
        message = annotator == null ? message : annotator.apply(ctx, message);
        preprocessAndHandlePoint(
            message, histogramDecoder, histogramHandler, preprocessorSupplier, ctx, "histogram");
        return;
      case LOGS_JSON_ARR:
      case LOGS_JSON_LINES:
      case LOGS_JSON_CLOUDWATCH:
        receivedLogsCounter.get(format).inc();
        if (isFeatureDisabled(logsDisabled, LOGS_DISABLED, discardedLogsCounter.get(format)))
          return;
        ReportableEntityHandler<ReportLog, ReportLog> logHandler = logHandlerSupplier.get();
        if (logHandler == null || logDecoder == null) {
          wavefrontHandler.reject(message, "Port is not configured to accept log data!");
          return;
        }
        logHandler.setLogFormat(format);
        message = annotator == null ? message : annotator.apply(ctx, message, true);
        preprocessAndHandleLog(message, logDecoder, logHandler, preprocessorSupplier, ctx);
        return;
      default:
        message = annotator == null ? message : annotator.apply(ctx, message);
        preprocessAndHandlePoint(
            message, wavefrontDecoder, wavefrontHandler, preprocessorSupplier, ctx, "metric");
    }
  }

  public static void preprocessAndHandlePoint(
      String message,
      ReportableEntityDecoder<String, ReportPoint> decoder,
      ReportableEntityHandler<ReportPoint, String> handler,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      @Nullable ChannelHandlerContext ctx,
      String type) {
    ReportableEntityPreprocessor preprocessor =
        preprocessorSupplier == null ? null : preprocessorSupplier.get();
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

    List<ReportPoint> output = new ArrayList<>(1);
    try {
      decoder.decode(message, output, "dummy");
    } catch (Exception e) {
      handler.reject(
          message,
          formatErrorMessage("WF-300 Cannot parse " + type + ": \"" + message + "\"", e, ctx));
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

  public static void preprocessAndHandleLog(
      String message,
      ReportableEntityDecoder<String, ReportLog> decoder,
      ReportableEntityHandler<ReportLog, ReportLog> handler,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      @Nullable ChannelHandlerContext ctx) {
    ReportableEntityPreprocessor preprocessor =
        preprocessorSupplier == null ? null : preprocessorSupplier.get();

    String[] messageHolder = new String[1];
    // transform the line if needed
    if (preprocessor != null) {
      message = preprocessor.forPointLine().transform(message);
      // apply white/black lists after formatting
      if (!preprocessor.forPointLine().filter(message, messageHolder)) {
        if (messageHolder[0] != null) {
          handler.reject((ReportLog) null, message);
        } else {
          handler.block(null, message);
        }
        return;
      }
    }

    List<ReportLog> output = new ArrayList<>(1);
    try {
      decoder.decode(message, output, "dummy");
    } catch (Exception e) {
      handler.reject(
          message, formatErrorMessage("WF-600 Cannot parse Log: \"" + message + "\"", e, ctx));
      return;
    }

    if (output.get(0) == null) {
      handler.reject(
          message, formatErrorMessage("WF-600 Cannot parse Log: \"" + message + "\"", null, ctx));
      return;
    }

    for (ReportLog object : output) {
      if (preprocessor != null) {
        preprocessor.forReportLog().transform(object);
        if (!preprocessor.forReportLog().filter(object, messageHolder)) {
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
