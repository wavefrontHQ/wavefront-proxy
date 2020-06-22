package com.wavefront.agent.listeners.tracing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import com.fasterxml.jackson.databind.JsonNode;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportableEntityDecoder;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.common.WavefrontSender;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

import static com.wavefront.internal.SpanDerivedMetricsUtils.reportHeartbeats;
import static com.wavefront.internal.SpanDerivedMetricsUtils.reportWavefrontGeneratedData;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.COMPONENT_TAG_KEY;
import static com.wavefront.sdk.common.Constants.ERROR_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * Handler that process trace data sent from tier 1 SDK.
 *
 * @author djia@vmware.com
 */
@ChannelHandler.Sharable
public class CustomTracingPortUnificationHandler extends TracePortUnificationHandler {
  private static final Logger logger = Logger.getLogger(
      CustomTracingPortUnificationHandler.class.getCanonicalName());
  @Nullable
  private final WavefrontSender wfSender;
  private final WavefrontInternalReporter wfInternalReporter;
  private final Set<Pair<Map<String, String>, String>> discoveredHeartbeatMetrics;
  private final Set<String> traceDerivedCustomTagKeys;
  private final String proxyLevelApplicationName;
  private final String proxyLevelServiceName;

  /**
   * @param handle                    handle/port number.
   * @param tokenAuthenticator        {@link TokenAuthenticator} for incoming requests.
   * @param healthCheckManager        shared health check endpoint handler.
   * @param traceDecoder              trace decoders.
   * @param spanLogsDecoder           span logs decoders.
   * @param preprocessor              preprocessor.
   * @param handlerFactory            factory for ReportableEntityHandler objects.
   * @param sampler                   sampler.
   * @param traceDisabled             supplier for backend-controlled feature flag for spans.
   * @param spanLogsDisabled          supplier for backend-controlled feature flag for span logs.
   * @param wfSender                  sender to send trace to Wavefront.
   * @param traceDerivedCustomTagKeys custom tags added to derived RED metrics.
   */
  public CustomTracingPortUnificationHandler(
      String handle, TokenAuthenticator tokenAuthenticator, HealthCheckManager healthCheckManager,
      ReportableEntityDecoder<String, Span> traceDecoder,
      ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDecoder,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessor,
      ReportableEntityHandlerFactory handlerFactory, SpanSampler sampler,
      Supplier<Boolean> traceDisabled, Supplier<Boolean> spanLogsDisabled,
      @Nullable WavefrontSender wfSender, @Nullable WavefrontInternalReporter wfInternalReporter,
      Set<String> traceDerivedCustomTagKeys, @Nullable String customTracingApplicationName,
      @Nullable String customTracingServiceName) {
    this(handle, tokenAuthenticator, healthCheckManager, traceDecoder, spanLogsDecoder,
        preprocessor, handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)),
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE_SPAN_LOGS, handle)),
        sampler, traceDisabled, spanLogsDisabled, wfSender, wfInternalReporter,
        traceDerivedCustomTagKeys, customTracingApplicationName, customTracingServiceName);
  }

  @VisibleForTesting
  public CustomTracingPortUnificationHandler(
      String handle, TokenAuthenticator tokenAuthenticator, HealthCheckManager healthCheckManager,
      ReportableEntityDecoder<String, Span> traceDecoder,
      ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDecoder,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessor,
      final ReportableEntityHandler<Span, String> handler,
      final ReportableEntityHandler<SpanLogs, String> spanLogsHandler, SpanSampler sampler,
      Supplier<Boolean> traceDisabled, Supplier<Boolean> spanLogsDisabled,
      @Nullable WavefrontSender wfSender, @Nullable WavefrontInternalReporter wfInternalReporter,
      Set<String> traceDerivedCustomTagKeys, @Nullable String customTracingApplicationName,
      @Nullable String customTracingServiceName) {
    super(handle, tokenAuthenticator, healthCheckManager, traceDecoder, spanLogsDecoder,
        preprocessor, handler, spanLogsHandler, sampler, traceDisabled, spanLogsDisabled);
    this.wfSender = wfSender;
    this.wfInternalReporter = wfInternalReporter;
    this.discoveredHeartbeatMetrics = Sets.newConcurrentHashSet();
    this.traceDerivedCustomTagKeys = traceDerivedCustomTagKeys;
    this.proxyLevelApplicationName = StringUtils.isBlank(customTracingApplicationName) ?
            "defaultApp" : customTracingApplicationName;
    this.proxyLevelServiceName = StringUtils.isBlank(customTracingServiceName) ?
            "defaultService" : customTracingServiceName;
  }

  @Override
  protected void report(Span object) {
    // report converted metrics/histograms from the span
    String applicationName = null;
    String serviceName = null;
    String cluster = NULL_TAG_VAL;
    String shard = NULL_TAG_VAL;
    String componentTagValue = NULL_TAG_VAL;
    String isError = "false";
    List<Annotation> annotations = object.getAnnotations();
    for (Annotation annotation : annotations) {
      switch (annotation.getKey()) {
        case APPLICATION_TAG_KEY:
          applicationName = annotation.getValue();
          continue;
        case SERVICE_TAG_KEY:
          serviceName = annotation.getValue();
          continue;
        case CLUSTER_TAG_KEY:
          cluster = annotation.getValue();
          continue;
        case SHARD_TAG_KEY:
          shard = annotation.getValue();
          continue;
        case COMPONENT_TAG_KEY:
          componentTagValue = annotation.getValue();
          continue;
        case ERROR_TAG_KEY:
          isError = annotation.getValue();
          continue;
      }
    }
    if (applicationName == null || serviceName == null) {
      logger.warning("Ingested spans discarded because span application/service name is " +
          "missing.");
      discardedSpans.inc();
      return;
    }
    handler.report(object);
    // update application and service for red metrics
    applicationName = firstNonNull(applicationName, proxyLevelApplicationName);
    serviceName = firstNonNull(serviceName, proxyLevelServiceName);
    if (wfInternalReporter != null) {
      List<Pair<String, String>> spanTags = annotations.stream().map(a -> new Pair<>(a.getKey(),
          a.getValue())).collect(Collectors.toList());
      discoveredHeartbeatMetrics.add(reportWavefrontGeneratedData(wfInternalReporter,
          object.getName(), applicationName, serviceName, cluster, shard, object.getSource(),
          componentTagValue, Boolean.parseBoolean(isError), object.getDuration(),
          traceDerivedCustomTagKeys, spanTags, true));
      try {
        reportHeartbeats(wfSender, discoveredHeartbeatMetrics, "wavefront-generated");
      } catch (IOException e) {
        logger.log(Level.WARNING, "Cannot report heartbeat metric to wavefront");
      }
    }
  }
}
