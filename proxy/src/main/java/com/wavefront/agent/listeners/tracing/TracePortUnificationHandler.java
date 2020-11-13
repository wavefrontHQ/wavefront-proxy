package com.wavefront.agent.listeners.tracing;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.databind.JsonNode;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.formatter.DataFormat;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.listeners.AbstractLineDelimitedHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportableEntityDecoder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.net.URI;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.CharsetUtil;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

import static com.wavefront.agent.listeners.FeatureCheckUtils.SPANLOGS_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.SPAN_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.isFeatureDisabled;
import static com.wavefront.agent.listeners.tracing.SpanUtils.handleSpanLogs;
import static com.wavefront.agent.listeners.tracing.SpanUtils.preprocessAndHandleSpan;

/**
 * Process incoming trace-formatted data.
 *
 * Accepts incoming messages of either String or FullHttpRequest type: single Span in a string, or
 * multiple points in the HTTP post body, newline-delimited.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class TracePortUnificationHandler extends AbstractLineDelimitedHandler {

  protected final ReportableEntityHandler<Span, String> handler;
  private final ReportableEntityHandler<SpanLogs, String> spanLogsHandler;
  private final ReportableEntityDecoder<String, Span> decoder;
  private final ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDecoder;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final SpanSampler sampler;
  private final Supplier<Boolean> traceDisabled;
  private final Supplier<Boolean> spanLogsDisabled;

  protected final Counter discardedSpans;
  protected final Counter discardedSpanLogs;
  private final Counter discardedSpansBySampler;
  private final Counter discardedSpanLogsBySampler;
  private final Counter receivedSpansTotal;


  public TracePortUnificationHandler(
      final String handle, final TokenAuthenticator tokenAuthenticator,
      final HealthCheckManager healthCheckManager,
      final ReportableEntityDecoder<String, Span> traceDecoder,
      final ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDecoder,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor,
      final ReportableEntityHandlerFactory handlerFactory, final SpanSampler sampler,
      final Supplier<Boolean> traceDisabled, final Supplier<Boolean> spanLogsDisabled) {
    this(handle, tokenAuthenticator, healthCheckManager, traceDecoder, spanLogsDecoder,
        preprocessor, handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)),
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE_SPAN_LOGS, handle)),
        sampler, traceDisabled, spanLogsDisabled);
  }

  @VisibleForTesting
  public TracePortUnificationHandler(
      final String handle, final TokenAuthenticator tokenAuthenticator,
      final HealthCheckManager healthCheckManager,
      final ReportableEntityDecoder<String, Span> traceDecoder,
      final ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDecoder,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor,
      final ReportableEntityHandler<Span, String> handler,
      final ReportableEntityHandler<SpanLogs, String> spanLogsHandler,
      final SpanSampler sampler, final Supplier<Boolean> traceDisabled,
      final Supplier<Boolean> spanLogsDisabled) {
    super(tokenAuthenticator, healthCheckManager, handle);
    this.decoder = traceDecoder;
    this.spanLogsDecoder = spanLogsDecoder;
    this.handler = handler;
    this.spanLogsHandler = spanLogsHandler;
    this.preprocessorSupplier = preprocessor;
    this.sampler = sampler;
    this.traceDisabled = traceDisabled;
    this.spanLogsDisabled = spanLogsDisabled;
    this.discardedSpans = Metrics.newCounter(new MetricName("spans." + handle, "", "discarded"));
    this.discardedSpanLogs = Metrics.newCounter(new MetricName("spanLogs." + handle, "",
        "discarded"));
    this.discardedSpansBySampler = Metrics.newCounter(new MetricName("spans." + handle, "",
        "sampler.discarded"));
    this.discardedSpanLogsBySampler = Metrics.newCounter(new MetricName("spanLogs." + handle, "",
        "sampler.discarded"));
    this.receivedSpansTotal = Metrics.newCounter(new MetricName("spans." + handle, "", "received.total"));
  }

  @Nullable
  @Override
  protected DataFormat getFormat(FullHttpRequest httpRequest) {
    return DataFormat.parse(URLEncodedUtils.parse(URI.create(httpRequest.uri()), CharsetUtil.UTF_8).
        stream().filter(x -> x.getName().equals("format") || x.getName().equals("f")).
        map(NameValuePair::getValue).findFirst().orElse(null));
  }

  @Override
  protected void processLine(final ChannelHandlerContext ctx, @Nonnull String message,
                             @Nullable DataFormat format) {
    if (format == DataFormat.SPAN_LOG || (message.startsWith("{") && message.endsWith("}"))) {
      if (isFeatureDisabled(spanLogsDisabled, SPANLOGS_DISABLED, discardedSpanLogs)) return;
      handleSpanLogs(message, spanLogsDecoder, decoder, spanLogsHandler, preprocessorSupplier,
          ctx, span -> sampler.sample(span, discardedSpanLogsBySampler));
      return;
    }

    // Payload is a span.
    receivedSpansTotal.inc();
    if (isFeatureDisabled(traceDisabled, SPAN_DISABLED, discardedSpans)) return;
    preprocessAndHandleSpan(message, decoder, handler, this::report, preprocessorSupplier, ctx,
        span -> sampler.sample(span, discardedSpansBySampler));
  }

  /**
   * Report span and derived metrics if needed.
   *
   * @param object span.
   */
  protected void report(Span object) {
    handler.report(object);
  }
}
