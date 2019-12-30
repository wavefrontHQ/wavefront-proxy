package com.wavefront.agent.listeners.tracing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.listeners.AbstractLineDelimitedHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportableEntityDecoder;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.entities.tracing.sampling.Sampler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import wavefront.report.Annotation;
import wavefront.report.Span;

import static com.wavefront.agent.channel.ChannelUtils.formatErrorMessage;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_KEY;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.reportHeartbeats;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.reportWavefrontGeneratedData;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.COMPONENT_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;

/**
 * Handler that process trace data sent from tier 1 SDK.
 *
 * @author djia@vmware.com
 */
@ChannelHandler.Sharable
public class CustomTracingPortUnificationHandler extends AbstractLineDelimitedHandler {
  private static final Logger logger = Logger.getLogger(
      CustomTracingPortUnificationHandler.class.getCanonicalName());

  private final ReportableEntityHandler<Span> handler;
  @Nullable
  private final WavefrontSender wfSender;
  private final WavefrontInternalReporter wfInternalReporter;
  private final ConcurrentMap<HeartbeatMetricKey, Boolean> discoveredHeartbeatMetrics;
  private final ReportableEntityDecoder<String, Span> decoder;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final Sampler sampler;
  private final boolean alwaysSampleErrors;
  private final Supplier<Boolean> traceDisabled;
  private final RateLimiter warningLoggerRateLimiter = RateLimiter.create(0.2);

  private final Counter discardedSpans;
  private final Counter discardedSpansBySampler;

  public CustomTracingPortUnificationHandler(
      final String handle, final TokenAuthenticator tokenAuthenticator,
      final HealthCheckManager healthCheckManager,
      @Nullable WavefrontSender wfSender,
      final ReportableEntityDecoder<String, Span> traceDecoder,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor,
      final ReportableEntityHandlerFactory handlerFactory, final Sampler sampler,
      final boolean alwaysSampleErrors, final Supplier<Boolean> traceDisabled) {
    this(handle, tokenAuthenticator, healthCheckManager, wfSender, traceDecoder,
        preprocessor, handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)),
        sampler, alwaysSampleErrors, traceDisabled);
  }

  @VisibleForTesting
  public CustomTracingPortUnificationHandler(
      final String handle, final TokenAuthenticator tokenAuthenticator,
      final HealthCheckManager healthCheckManager,
      @Nullable WavefrontSender wfSender,
      final ReportableEntityDecoder<String, Span> traceDecoder,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor,
      final ReportableEntityHandler<Span> handler, final Sampler sampler,
      final boolean alwaysSampleErrors, final Supplier<Boolean> traceDisabled) {
    super(tokenAuthenticator, healthCheckManager, handle);
    this.decoder = traceDecoder;
    this.handler = handler;
    this.wfSender = wfSender;
    this.discoveredHeartbeatMetrics = new ConcurrentHashMap<>();
    this.preprocessorSupplier = preprocessor;
    this.sampler = sampler;
    this.alwaysSampleErrors = alwaysSampleErrors;
    this.traceDisabled = traceDisabled;
    this.discardedSpans = Metrics.newCounter(new MetricName("spans." + handle, "", "discarded"));
    this.discardedSpansBySampler = Metrics.newCounter(new MetricName("spans." + handle, "",
        "sampler.discarded"));
    if (wfSender != null) {
      wfInternalReporter = new WavefrontInternalReporter.Builder().
          prefixedWith("tracing.derived").withSource("custom_tracing").reportMinuteDistribution().
          build(wfSender);
      // Start the reporter
      wfInternalReporter.start(1, TimeUnit.MINUTES);
    } else {
      wfInternalReporter = null;
    }
  }

  @Override
  protected void processLine(ChannelHandlerContext ctx, String message) {
    if (traceDisabled.get()) {
      if (warningLoggerRateLimiter.tryAcquire()) {
        logger.warning("Ingested spans discarded because tracing feature is not enabled on the " +
            "server");
      }
      discardedSpans.inc();
      return;
    }

    ReportableEntityPreprocessor preprocessor = preprocessorSupplier == null ?
        null : preprocessorSupplier.get();
    String[] messageHolder = new String[1];

    // transform the line if needed
    if (preprocessor != null) {
      message = preprocessor.forPointLine().transform(message);

      if (!preprocessor.forPointLine().filter(message, messageHolder)) {
        if (messageHolder[0] != null) {
          handler.reject((Span) null, messageHolder[0]);
        } else {
          handler.block(null, message);
        }
        return;
      }
    }

    List<Span> output = Lists.newArrayListWithCapacity(1);
    try {
      decoder.decode(message, output, "dummy");
    } catch (Exception e) {
      handler.reject(message, formatErrorMessage(message, e, ctx));
      return;
    }

    for (Span object : output) {
      if (preprocessor != null) {
        preprocessor.forSpan().transform(object);
        if (!preprocessor.forSpan().filter(object, messageHolder)) {
          if (messageHolder[0] != null) {
            handler.reject(object, messageHolder[0]);
          } else {
            handler.block(object);
          }
          return;
        }
      }

      // check whether error span tag exists.
      boolean sampleError = alwaysSampleErrors && object.getAnnotations().stream().anyMatch(
          t -> t.getKey().equals(ERROR_SPAN_TAG_KEY) && t.getValue().equals(ERROR_SPAN_TAG_VAL));
      if (sampleError || sample(object)) {
        handler.report(object);

        // report converted metrics/histograms from the span
        String applicationName = "wfProxy";
        String serviceName = "defaultService";
        String cluster = NULL_TAG_VAL;
        String shard = NULL_TAG_VAL;
        String componentTagValue = NULL_TAG_VAL;
        String isError = "false";
        if (wfInternalReporter != null) {
          List<Annotation> annotations = object.getAnnotations();
          for (Annotation annotation : annotations) {
            switch (annotation.getKey()) {
              case APPLICATION_TAG_KEY:
                applicationName = annotation.getValue();
                continue;
              case SERVICE_TAG_KEY:
                serviceName = annotation.getValue();
              case CLUSTER_TAG_KEY:
                cluster = annotation.getValue();
                continue;
              case SHARD_TAG_KEY:
                shard = annotation.getValue();
                continue;
              case COMPONENT_TAG_KEY:
                componentTagValue = annotation.getValue();
                break;
              case ERROR_SPAN_TAG_KEY:
                isError = annotation.getValue();
                break;
            }
          }
          discoveredHeartbeatMetrics.putIfAbsent(reportWavefrontGeneratedData(wfInternalReporter,
              object.getName(), applicationName, serviceName, cluster, shard, object.getSource(),
              componentTagValue, Boolean.parseBoolean(isError), object.getDuration(),  new HashSet<>(),
              annotations), true);
        }

        try {
          reportHeartbeats("customTracing", wfSender, discoveredHeartbeatMetrics);
        } catch (IOException e) {
          logger.log(Level.WARNING, "Cannot report heartbeat metric to wavefront");
        }
      }
    }
  }

  private boolean sample(Span object) {
    if (sampler.sample(object.getName(),
        UUID.fromString(object.getTraceId()).getLeastSignificantBits(), object.getDuration())) {
      return true;
    }
    discardedSpansBySampler.inc();
    return false;
  }
}
