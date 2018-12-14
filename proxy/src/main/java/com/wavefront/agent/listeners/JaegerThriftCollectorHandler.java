package com.wavefront.agent.listeners;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

import com.uber.tchannel.api.handlers.ThriftRequestHandler;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
import wavefront.report.Span;

/**
 * Handler that processes trace data in Jaeger Thrift compact format and converts them to Wavefront format
 *
 * @author vasily@wavefront.com
 */
public class JaegerThriftCollectorHandler extends ThriftRequestHandler<Collector.submitBatches_args,
    Collector.submitBatches_result> {
  protected static final Logger logger = Logger.getLogger(JaegerThriftCollectorHandler.class.getCanonicalName());

  // TODO: support sampling
  private final static Set<String> IGNORE_TAGS = ImmutableSet.of("sampler.type", "sampler.param");
  private final static String APPLICATION_KEY = "application";
  private final static String SERVICE_KEY = "service";
  private final static String DEFAULT_APPLICATION = "Jaeger";
  private static final Logger jaegerDataLogger = Logger.getLogger("JaegerDataLogger");

  private final String handle;
  private final ReportableEntityHandler<Span> handler;
  private final AtomicBoolean traceDisabled;

  // log every 5 seconds
  private final RateLimiter warningLoggerRateLimiter = RateLimiter.create(0.2);

  private final Counter discardedTraces;
  private final Counter discardedBatches;
  private final Counter processedBatches;
  private final Counter failedBatches;

  @SuppressWarnings("unchecked")
  public JaegerThriftCollectorHandler(String handle, ReportableEntityHandlerFactory handlerFactory,
                                      AtomicBoolean traceDisabled) {
    this(handle, handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)), traceDisabled);
  }

  public JaegerThriftCollectorHandler(String handle, ReportableEntityHandler<Span> handler,
                                      AtomicBoolean traceDisabled) {
    this.handle = handle;
    this.handler = handler;
    this.traceDisabled = traceDisabled;
    this.discardedTraces = Metrics.newCounter(new MetricName("spans." + handle, "", "discarded"));
    this.discardedBatches = Metrics.newCounter(new MetricName("spans." + handle + ".batches", "", "discarded"));
    this.processedBatches = Metrics.newCounter(new MetricName("spans." + handle + ".batches", "", "processed"));
    this.failedBatches = Metrics.newCounter(new MetricName("spans." + handle + ".batches", "", "failed"));
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
        logger.log(Level.WARNING, "Jaeger Thrift batch processing failed", Throwables.getRootCause(e));
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
        logger.info("Ingested spans discarded because tracing feature is not enabled on the server");
      }
      discardedBatches.inc();
      discardedTraces.inc(batch.getSpansSize());
      return;
    }
    for (io.jaegertracing.thriftjava.Span span : batch.getSpans()) {
      processSpan(span, serviceName, sourceName);
    }
  }

  private void processSpan(io.jaegertracing.thriftjava.Span span, String serviceName, String sourceName) {
    List<Annotation> annotations = new ArrayList<>();
    // serviceName is mandatory in Jaeger
    annotations.add(new Annotation(SERVICE_KEY, serviceName));
    long parentSpanId = span.getParentSpanId();
    if (parentSpanId > 0) {
      annotations.add(new Annotation("parent", new UUID(0, parentSpanId).toString()));
    }

    boolean applicationTagPresent = false;
    if (span.getTags() != null) {
      for (Tag tag : span.getTags()) {
        if (applicationTagPresent || tag.getKey().equals(APPLICATION_KEY)) {
          applicationTagPresent = true;
        }
        if (IGNORE_TAGS.contains(tag.getKey())) {
          continue;
        }
        Annotation annotation = tagToAnnotation(tag);
        if (annotation != null) {
          annotations.add(annotation);
        }
      }
    }

    if (!applicationTagPresent) {
      // Original Jaeger span did not have application set, will default to 'Jaeger'
      Annotation annotation = new Annotation(APPLICATION_KEY, DEFAULT_APPLICATION);
      annotations.add(annotation);
    }

    if (span.getReferences() != null) {
      for (SpanRef reference : span.getReferences()) {
        switch (reference.refType) {
          case CHILD_OF:
            if (reference.getSpanId() != 0 && reference.getSpanId() != parentSpanId) {
              annotations.add(new Annotation("parent", new UUID(0, reference.getSpanId()).toString()));
            }
          case FOLLOWS_FROM:
            if (reference.getSpanId() != 0) {
              annotations.add(new Annotation("followsFrom", new UUID(0, reference.getSpanId()).toString()));
            }
          default:
        }
      }
    }
    Span newSpan = Span.newBuilder()
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
    if (jaegerDataLogger.isLoggable(Level.FINEST)) {
      logger.info("JaegerToWavefrontSpan: " + span.toString());
      logger.info("JaegerToWavefrontSpan: " + newSpan.toString());
    }

    handler.report(newSpan);
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
}
