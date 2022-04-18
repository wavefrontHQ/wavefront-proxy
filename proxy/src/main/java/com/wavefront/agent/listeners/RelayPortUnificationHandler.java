package com.wavefront.agent.listeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.wavefront.agent.ProxyConfig;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.channel.SharedGraphiteHostAnnotator;
import com.wavefront.agent.formatter.DataFormat;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.api.agent.Constants;
import com.wavefront.common.Utils;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportableEntityDecoder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import wavefront.report.ReportPoint;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

import static com.wavefront.agent.channel.ChannelUtils.formatErrorMessage;
import static com.wavefront.agent.channel.ChannelUtils.errorMessageWithRootCause;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static com.wavefront.agent.listeners.FeatureCheckUtils.HISTO_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.SPANLOGS_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.SPAN_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.LOGS_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.isFeatureDisabled;
import javax.annotation.Nullable;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.wavefront.agent.channel.ChannelUtils.*;
import static com.wavefront.agent.listeners.FeatureCheckUtils.*;
import static com.wavefront.agent.listeners.WavefrontPortUnificationHandler.preprocessAndHandlePoint;

/**
 * A unified HTTP endpoint for mixed format data. Can serve as a proxy endpoint and process
 * incoming HTTP requests from other proxies (i.e. act as a relay for proxy chaining), as well as
 * serve as a DDI (Direct Data Ingestion) endpoint.
 * All the data received on this endpoint will register as originating from this proxy.
 * Supports metric, histogram and distributed trace data (no source tag support or log support at
 * this moment).
 * Intended for internal use.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class RelayPortUnificationHandler extends AbstractHttpOnlyHandler {
  private static final Logger logger = Logger.getLogger(
      RelayPortUnificationHandler.class.getCanonicalName());

  private static final ObjectMapper JSON_PARSER = new ObjectMapper();

  private final Map<ReportableEntityType, ReportableEntityDecoder<?, ?>> decoders;
  private final ReportableEntityDecoder<String, ReportPoint> wavefrontDecoder;
  private ProxyConfig proxyConfig;
  private final ReportableEntityHandler<ReportPoint, String> wavefrontHandler;
  private final Supplier<ReportableEntityHandler<ReportPoint, String>> histogramHandlerSupplier;
  private final Supplier<ReportableEntityHandler<Span, String>> spanHandlerSupplier;
  private final Supplier<ReportableEntityHandler<SpanLogs, String>> spanLogsHandlerSupplier;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final SharedGraphiteHostAnnotator annotator;

  private final Supplier<Boolean> histogramDisabled;
  private final Supplier<Boolean> traceDisabled;
  private final Supplier<Boolean> spanLogsDisabled;
  private final Supplier<Boolean> logsDisabled;

  private final Supplier<Counter> discardedHistograms;
  private final Supplier<Counter> discardedSpans;
  private final Supplier<Counter> discardedSpanLogs;
  private final Supplier<Counter> receivedSpansTotal;
  private final Supplier<Counter> discardedLogs;
  private final Supplier<Counter> receivedLogsTotal;

  private final APIContainer apiContainer;
  /**
   * Create new instance with lazy initialization for handlers.
   *
   * @param handle               handle/port number.
   * @param tokenAuthenticator   tokenAuthenticator for incoming requests.
   * @param healthCheckManager   shared health check endpoint handler.
   * @param decoders             decoders.
   * @param handlerFactory       factory for ReportableEntityHandler objects.
   * @param preprocessorSupplier preprocessor supplier.
   * @param histogramDisabled    supplier for backend-controlled feature flag for histograms.
   * @param traceDisabled        supplier for backend-controlled feature flag for spans.
   * @param spanLogsDisabled     supplier for backend-controlled feature flag for span logs.
   * @param logsDisabled         supplier for backend-controlled feature flag for logs.
   */
  @SuppressWarnings("unchecked")
  public RelayPortUnificationHandler(
      final String handle, final TokenAuthenticator tokenAuthenticator,
      final HealthCheckManager healthCheckManager,
      final Map<ReportableEntityType, ReportableEntityDecoder<?, ?>> decoders,
      final ReportableEntityHandlerFactory handlerFactory,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      @Nullable final SharedGraphiteHostAnnotator annotator,
      final Supplier<Boolean> histogramDisabled, final Supplier<Boolean> traceDisabled,
      final Supplier<Boolean> spanLogsDisabled,
      final Supplier<Boolean> logsDisabled,
      final APIContainer apiContainer,
      final ProxyConfig proxyConfig) {
    super(tokenAuthenticator, healthCheckManager, handle);
    this.decoders = decoders;
    this.wavefrontDecoder = (ReportableEntityDecoder<String, ReportPoint>) decoders.
        get(ReportableEntityType.POINT);
    this.proxyConfig = proxyConfig;
    this.wavefrontHandler = handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT,
        handle));
    this.histogramHandlerSupplier = Utils.lazySupplier(() -> handlerFactory.getHandler(
        HandlerKey.of(ReportableEntityType.HISTOGRAM, handle)));
    this.spanHandlerSupplier = Utils.lazySupplier(() -> handlerFactory.getHandler(HandlerKey.of(
        ReportableEntityType.TRACE, handle)));
    this.spanLogsHandlerSupplier = Utils.lazySupplier(() -> handlerFactory.getHandler(HandlerKey.of(
        ReportableEntityType.TRACE_SPAN_LOGS, handle)));
    this.receivedSpansTotal = Utils.lazySupplier(() -> Metrics.newCounter(new MetricName(
        "spans." + handle, "", "received.total")));
    this.preprocessorSupplier = preprocessorSupplier;
    this.annotator = annotator;
    this.histogramDisabled = histogramDisabled;
    this.traceDisabled = traceDisabled;
    this.spanLogsDisabled = spanLogsDisabled;
    this.logsDisabled = logsDisabled;

    this.discardedHistograms = Utils.lazySupplier(() -> Metrics.newCounter(new MetricName(
        "histogram", "", "discarded_points")));
    this.discardedSpans = Utils.lazySupplier(() -> Metrics.newCounter(new MetricName(
        "spans." + handle, "", "discarded")));
    this.discardedSpanLogs = Utils.lazySupplier(() -> Metrics.newCounter(new MetricName(
        "spanLogs." + handle, "", "discarded")));
    this.discardedLogs = Utils.lazySupplier(() -> Metrics.newCounter(new MetricName(
        "logs." + handle, "", "discarded")));
    this.receivedLogsTotal = Utils.lazySupplier(() -> Metrics.newCounter(new MetricName(
        "logs." + handle, "", "received.total")));

    this.apiContainer = apiContainer;
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) {
    URI uri = URI.create(request.uri());
    StringBuilder output = new StringBuilder();
    String path = uri.getPath();

    if (path.endsWith("/checkin") && (path.startsWith("/api/daemon") || path.contains("wfproxy"))) {
      Map<String, String> query = URLEncodedUtils.parse(uri, Charset.forName("UTF-8")).
              stream().collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue));

      String agentMetricsStr = request.content().toString(CharsetUtil.UTF_8);
      JsonNode agentMetrics;
      try {
        agentMetrics = JSON_PARSER.readTree(agentMetricsStr);
      } catch (JsonProcessingException e) {
        if (logger.isLoggable(Level.FINE)) {
          logger.log(Level.WARNING, "Exception: ", e);
        }
        agentMetrics = JsonNodeFactory.instance.objectNode();
      }

      try {
        AgentConfiguration agentConfiguration = apiContainer.getProxyV2APIForTenant(APIContainer.CENTRAL_TENANT_NAME).proxyCheckin(
                UUID.fromString(request.headers().get("X-WF-PROXY-ID")),
                "Bearer " + proxyConfig.getToken(),
                query.get("hostname"),
                query.get("version"),
                Long.parseLong(query.get("currentMillis")),
                agentMetrics,
                Boolean.parseBoolean(query.get("ephemeral"))
        );
        JsonNode node = JSON_PARSER.valueToTree(agentConfiguration);
        writeHttpResponse(ctx, HttpResponseStatus.OK, node, request);
      } catch (javax.ws.rs.ProcessingException e) {
        logger.warning("Problem while checking a chained proxy: " + e);
        if (logger.isLoggable(Level.FINE)) {
          logger.log(Level.WARNING, "Exception: ", e);
        }
        Throwable rootCause = Throwables.getRootCause(e);
        String error = "Request processing error: Unable to retrieve proxy configuration from '" + proxyConfig.getServer() + "' :" + rootCause;
        writeHttpResponse(ctx, new HttpResponseStatus(444, error), error, request);
      } catch (Throwable e) {
        logger.warning("Problem while checking a chained proxy: " + e);
        if (logger.isLoggable(Level.FINE)) {
          logger.log(Level.WARNING, "Exception: ", e);
        }
        String error = "Request processing error: Unable to retrieve proxy configuration from '" + proxyConfig.getServer() + "'";
        writeHttpResponse(ctx, new HttpResponseStatus(500, error), error, request);
      }
      return;
    }

    String format = URLEncodedUtils.parse(uri, CharsetUtil.UTF_8).stream().
        filter(x -> x.getName().equals("format") || x.getName().equals("f")).
        map(NameValuePair::getValue).findFirst().orElse(Constants.PUSH_FORMAT_WAVEFRONT);

    // Return HTTP 200 (OK) for payloads received on the proxy endpoint
    // Return HTTP 202 (ACCEPTED) for payloads received on the DDI endpoint
    // Return HTTP 204 (NO_CONTENT) for payloads received on all other endpoints
    final boolean isDirectIngestion = path.startsWith("/report");
    HttpResponseStatus okStatus;
    if (isDirectIngestion) {
      okStatus = HttpResponseStatus.ACCEPTED;
    } else if (path.contains("/pushdata/") || path.contains("wfproxy/report")) {
      okStatus = HttpResponseStatus.OK;
    } else {
      okStatus = HttpResponseStatus.NO_CONTENT;
    }

    HttpResponseStatus status;
    switch (format) {
      case Constants.PUSH_FORMAT_HISTOGRAM:
        if (isFeatureDisabled(histogramDisabled, HISTO_DISABLED, discardedHistograms.get(),
                output, request)) {
          status = HttpResponseStatus.FORBIDDEN;
          break;
        }
      case Constants.PUSH_FORMAT_WAVEFRONT:
      case Constants.PUSH_FORMAT_GRAPHITE_V2:
        AtomicBoolean hasSuccessfulPoints = new AtomicBoolean(false);
        try {
          //noinspection unchecked
          ReportableEntityDecoder<String, ReportPoint> histogramDecoder =
              (ReportableEntityDecoder<String, ReportPoint>) decoders.
                  get(ReportableEntityType.HISTOGRAM);
          Splitter.on('\n').trimResults().omitEmptyStrings().
              split(request.content().toString(CharsetUtil.UTF_8)).forEach(message -> {
            DataFormat dataFormat = DataFormat.autodetect(message);
            switch (dataFormat) {
              case EVENT:
                wavefrontHandler.reject(message, "Relay port does not support " +
                    "event-formatted data!");
                break;
              case SOURCE_TAG:
                wavefrontHandler.reject(message, "Relay port does not support " +
                    "sourceTag-formatted data!");
                break;
              case HISTOGRAM:
                if (isFeatureDisabled(histogramDisabled, HISTO_DISABLED,
                    discardedHistograms.get(), output)) {
                  break;
                }
                preprocessAndHandlePoint(message, histogramDecoder, histogramHandlerSupplier.get(),
                    preprocessorSupplier, ctx, "histogram");
                hasSuccessfulPoints.set(true);
                break;
              default:
                // only apply annotator if point received on the DDI endpoint
                message = annotator != null && isDirectIngestion ?
                    annotator.apply(ctx, message) : message;
                preprocessAndHandlePoint(message, wavefrontDecoder, wavefrontHandler,
                    preprocessorSupplier, ctx, "metric");
                hasSuccessfulPoints.set(true);
                break;
            }
          });
          status = hasSuccessfulPoints.get() ? okStatus : HttpResponseStatus.BAD_REQUEST;
        } catch (Exception e) {
          status = HttpResponseStatus.BAD_REQUEST;
          output.append(errorMessageWithRootCause(e));
          logWarning("WF-300: Failed to handle HTTP POST", e, ctx);
        }
        break;
      case Constants.PUSH_FORMAT_TRACING:
        if (isFeatureDisabled(traceDisabled, SPAN_DISABLED, discardedSpans.get(), output,
            request)) {
          receivedSpansTotal.get().inc(discardedSpans.get().count());
          status = HttpResponseStatus.FORBIDDEN;
          break;
        }
        List<Span> spans = new ArrayList<>();
        //noinspection unchecked
        ReportableEntityDecoder<String, Span> spanDecoder =
            (ReportableEntityDecoder<String, Span>) decoders.
                get(ReportableEntityType.TRACE);
        ReportableEntityHandler<Span, String> spanHandler = spanHandlerSupplier.get();
        Splitter.on('\n').trimResults().omitEmptyStrings().
            split(request.content().toString(CharsetUtil.UTF_8)).forEach(line -> {
          try {
            receivedSpansTotal.get().inc();
            spanDecoder.decode(line, spans, "dummy");
          } catch (Exception e) {
            spanHandler.reject(line, formatErrorMessage(line, e, ctx));
          }
        });
        spans.forEach(spanHandler::report);
        status = okStatus;
        break;
      case Constants.PUSH_FORMAT_TRACING_SPAN_LOGS:
        if (isFeatureDisabled(spanLogsDisabled, SPANLOGS_DISABLED, discardedSpanLogs.get(),
            output, request)) {
          status = HttpResponseStatus.FORBIDDEN;
          break;
        }
        List<SpanLogs> spanLogs = new ArrayList<>();
        //noinspection unchecked
        ReportableEntityDecoder<JsonNode, SpanLogs> spanLogDecoder =
            (ReportableEntityDecoder<JsonNode, SpanLogs>) decoders.
                get(ReportableEntityType.TRACE_SPAN_LOGS);
        ReportableEntityHandler<SpanLogs, String> spanLogsHandler = spanLogsHandlerSupplier.get();
        Splitter.on('\n').trimResults().omitEmptyStrings().
            split(request.content().toString(CharsetUtil.UTF_8)).forEach(line -> {
          try {
            spanLogDecoder.decode(JSON_PARSER.readTree(line), spanLogs, "dummy");
          } catch (Exception e) {
            spanLogsHandler.reject(line, formatErrorMessage(line, e, ctx));
          }
        });
        spanLogs.forEach(spanLogsHandler::report);
        status = okStatus;
        break;
      case Constants.PUSH_FORMAT_LOGS_JSON_ARR:
        if (isFeatureDisabled(logsDisabled, LOGS_DISABLED, discardedLogs.get(),
            output, request)) {
          status = HttpResponseStatus.FORBIDDEN;
          break;
        }
      default:
        status = HttpResponseStatus.BAD_REQUEST;
        logger.warning("Unexpected format for incoming HTTP request: " + format);
    }
    writeHttpResponse(ctx, status, output, request);
  }
}
