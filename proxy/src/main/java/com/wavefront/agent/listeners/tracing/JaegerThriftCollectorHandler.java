package com.wavefront.agent.listeners.tracing;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

import com.uber.tchannel.api.handlers.ThriftRequestHandler;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TraceConstants;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.jaegertracing.thriftjava.Batch;
import io.jaegertracing.thriftjava.Collector;
import io.jaegertracing.thriftjava.SpanRef;
import io.jaegertracing.thriftjava.Tag;
import io.jaegertracing.thriftjava.TagType;
import wavefront.report.Annotation;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_KEY;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.reportHeartbeats;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.reportWavefrontGeneratedData;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;

/**
 * Handler that processes trace data in Jaeger Thrift compact format and
 * converts them to Wavefront format
 *
 * @author vasily@wavefront.com
 */
public class JaegerThriftCollectorHandler extends ThriftRequestHandler<Collector.submitBatches_args,
    Collector.submitBatches_result> implements Runnable, Closeable {
  protected static final Logger logger =
      Logger.getLogger(JaegerThriftCollectorHandler.class.getCanonicalName());

  // TODO: support sampling
  private final static Set<String> IGNORE_TAGS = ImmutableSet.of("sampler.type",
      "sampler.param");
  private final static String JAEGER_COMPONENT = "jaeger";
  private final static String DEFAULT_APPLICATION = "Jaeger";
  private static final Logger JAEGER_DATA_LOGGER = Logger.getLogger("JaegerDataLogger");

  private final String handle;
  private final ReportableEntityHandler<Span> spanHandler;
  @Nullable
  private final ReportableEntityHandler<ReportPoint> pointHandler;
  private final AtomicBoolean traceDisabled;

  // log every 5 seconds
  private final RateLimiter warningLoggerRateLimiter = RateLimiter.create(0.2);

  private final Counter discardedTraces;
  private final Counter discardedBatches;
  private final Counter processedBatches;
  private final Counter failedBatches;
  private final ConcurrentMap<HeartbeatMetricKey, Boolean> discoveredHeartbeatMetrics;
  private final ScheduledExecutorService scheduledExecutorService;

  @SuppressWarnings("unchecked")
  public JaegerThriftCollectorHandler(String handle, ReportableEntityHandlerFactory handlerFactory,
                                      @Nullable ReportableEntityHandler<ReportPoint> pointHandler,
                                      AtomicBoolean traceDisabled) {
    this(handle, handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)),
        pointHandler, traceDisabled);
  }

  public JaegerThriftCollectorHandler(String handle,
                                      ReportableEntityHandler<Span> spanHandler,
                                      @Nullable ReportableEntityHandler<ReportPoint> pointHandler,
                                      AtomicBoolean traceDisabled) {
    this.handle = handle;
    this.spanHandler = spanHandler;
    this.pointHandler = pointHandler;
    this.traceDisabled = traceDisabled;
    this.discardedTraces = Metrics.newCounter(
        new MetricName("spans." + handle, "", "discarded"));
    this.discardedBatches = Metrics.newCounter(
        new MetricName("spans." + handle + ".batches", "", "discarded"));
    this.processedBatches = Metrics.newCounter(
        new MetricName("spans." + handle + ".batches", "", "processed"));
    this.failedBatches = Metrics.newCounter(
        new MetricName("spans." + handle + ".batches", "", "failed"));
    this.discoveredHeartbeatMetrics =  new ConcurrentHashMap<>();
    this.scheduledExecutorService = Executors.newScheduledThreadPool(1,
        new NamedThreadFactory("jaeger-heart-beater"));
    scheduledExecutorService.scheduleAtFixedRate(this, 1, 1, TimeUnit.MINUTES);
  }

  @Override
  public ThriftResponse<Collector.submitBatches_result> handleImpl(
      ThriftRequest<Collector.submitBatches_args> request) {
    for (Batch batch : request.getBody(Collector.submitBatches_args.class).getBatches()) {
      try {
        processBatch(batch);
        processedBatches.inc();
      } catch (Exception e) {
        failedBatches.inc();
        logger.log(Level.WARNING, "Jaeger Thrift batch processing failed",
            Throwables.getRootCause(e));
      }
    }
    return new ThriftResponse.Builder<Collector.submitBatches_result>(request)
        .setBody(new Collector.submitBatches_result())
        .build();
  }

  private void processBatch(Batch batch) {
    String serviceName = batch.getProcess().getServiceName();
    String sourceName = null;
    if (batch.getProcess().getTags() != null) {
      for (Tag tag : batch.getProcess().getTags()) {
        if (tag.getKey().equals("hostname") && tag.getVType() == TagType.STRING) {
          sourceName = tag.getVStr();
          break;
        }
        if (tag.getKey().equals("ip") && tag.getVType()== TagType.STRING) {
          sourceName = tag.getVStr();
        }
      }
      if (sourceName == null) {
        sourceName = "unknown";
      }
    }
    if (traceDisabled.get()) {
      if (warningLoggerRateLimiter.tryAcquire()) {
        logger.info("Ingested spans discarded because tracing feature is not " +
            "enabled on the server");
      }
      discardedBatches.inc();
      discardedTraces.inc(batch.getSpansSize());
      return;
    }
    for (io.jaegertracing.thriftjava.Span span : batch.getSpans()) {
      processSpan(span, serviceName, sourceName);
    }
  }

  private void processSpan(io.jaegertracing.thriftjava.Span span,
                           String serviceName,
                           String sourceName) {
    List<Annotation> annotations = new ArrayList<>();
    // serviceName is mandatory in Jaeger
    annotations.add(new Annotation(SERVICE_TAG_KEY, serviceName));
    long parentSpanId = span.getParentSpanId();
    if (parentSpanId != 0) {
      annotations.add(new Annotation("parent", new UUID(0, parentSpanId).toString()));
    }

    String applicationName = NULL_TAG_VAL;
    String cluster = NULL_TAG_VAL;
    String shard = NULL_TAG_VAL;
    boolean isError = false;

    boolean applicationTagPresent = false;
    if (span.getTags() != null) {
      for (Tag tag : span.getTags()) {
        if (applicationTagPresent || tag.getKey().equals(APPLICATION_TAG_KEY)) {
          applicationName = tag.getKey();
          applicationTagPresent = true;
        }
        if (IGNORE_TAGS.contains(tag.getKey())) {
          continue;
        }

        Annotation annotation = tagToAnnotation(tag);
        if (annotation != null) {
          annotations.add(annotation);

          switch (annotation.getKey()) {
            case CLUSTER_TAG_KEY:
              cluster = annotation.getValue();
              continue;
            case SHARD_TAG_KEY:
              shard = annotation.getValue();
              continue;
            case ERROR_SPAN_TAG_KEY:
              // only error=true is supported
              isError = annotation.getValue().equals(ERROR_SPAN_TAG_VAL);
          }
        }
      }
    }

    if (!applicationTagPresent) {
      // Original Jaeger span did not have application set, will default to 'Jaeger'
      applicationName = DEFAULT_APPLICATION;
      Annotation annotation = new Annotation(APPLICATION_TAG_KEY, DEFAULT_APPLICATION);
      annotations.add(annotation);
    }

    if (span.getReferences() != null) {
      for (SpanRef reference : span.getReferences()) {
        switch (reference.refType) {
          case CHILD_OF:
            if (reference.getSpanId() != 0 && reference.getSpanId() != parentSpanId) {
              annotations.add(new Annotation(TraceConstants.PARENT_KEY,
                  new UUID(0, reference.getSpanId()).toString()));
            }
          case FOLLOWS_FROM:
            if (reference.getSpanId() != 0) {
              annotations.add(new Annotation(TraceConstants.FOLLOWS_FROM_KEY,
                  new UUID(0, reference.getSpanId())
                  .toString()));
            }
          default:
        }
      }
    }
    Span wavefrontSpan = Span.newBuilder()
        .setCustomer("dummy")
        .setName(span.getOperationName())
        .setSource(sourceName)
        .setSpanId(new UUID(0, span.getSpanId()).toString())
        .setTraceId(new UUID(span.getTraceIdHigh(), span.getTraceIdLow()).toString())
        .setStartMillis(span.getStartTime() / 1000)
        .setDuration(span.getDuration() / 1000)
        .setAnnotations(annotations)
        .build();

    // Log Jaeger spans as well as Wavefront spans for debugging purposes.
    if (JAEGER_DATA_LOGGER.isLoggable(Level.FINEST)) {
      JAEGER_DATA_LOGGER.info("Inbound Jaeger span: " + span.toString());
      JAEGER_DATA_LOGGER.info("Converted Wavefront span: " + wavefrontSpan.toString());
    }

    spanHandler.report(wavefrontSpan);

    // report converted metrics/histograms from the span
    discoveredHeartbeatMetrics.putIfAbsent(reportWavefrontGeneratedData(span.getOperationName(),
        applicationName, serviceName, cluster, shard, sourceName, isError, span.getDuration()),
        true);
  }

  @Nullable
  private static Annotation tagToAnnotation(Tag tag) {
    switch (tag.vType) {
      case BOOL:
        return new Annotation(tag.getKey(), String.valueOf(tag.isVBool()));
      case LONG:
        return new Annotation(tag.getKey(), String.valueOf(tag.getVLong()));
      case DOUBLE:
        return new Annotation(tag.getKey(), String.valueOf(tag.getVDouble()));
      case STRING:
        return new Annotation(tag.getKey(), tag.getVStr());
      case BINARY:
      default:
        return null;
    }
  }

  @Override
  public void run() {
    reportHeartbeats(JAEGER_COMPONENT, pointHandler, discoveredHeartbeatMetrics);
  }

  @Override
  public void close() throws IOException {
    scheduledExecutorService.shutdownNow();
  }
}
