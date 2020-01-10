package com.wavefront.agent.listeners.tracing;

import com.google.common.collect.ImmutableSet;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.MessageDedupingLogger;
import com.wavefront.common.TraceConstants;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.sdk.entities.tracing.sampling.Sampler;
import com.yammer.metrics.core.Counter;
import io.jaegertracing.thriftjava.Batch;
import io.jaegertracing.thriftjava.SpanRef;
import io.jaegertracing.thriftjava.Tag;
import io.jaegertracing.thriftjava.TagType;
import org.apache.commons.lang.StringUtils;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

import javax.annotation.Nullable;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.DEBUG_SPAN_TAG_KEY;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_KEY;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.reportWavefrontGeneratedData;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.COMPONENT_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SOURCE_KEY;

/**
 * Utility methods for processing Jaeger Thrift trace data.
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public abstract class JaegerThriftUtils {
  protected static final Logger logger =
      Logger.getLogger(JaegerThriftUtils.class.getCanonicalName());
  private static final Logger featureDisabledLogger = new MessageDedupingLogger(logger, 2, 0.2);

  // TODO: support sampling
  private final static Set<String> IGNORE_TAGS = ImmutableSet.of("sampler.type", "sampler.param");
  private final static String FORCE_SAMPLED_KEY = "sampling.priority";
  private static final Logger JAEGER_DATA_LOGGER = Logger.getLogger("JaegerDataLogger");

  private JaegerThriftUtils() {
  }

  public static void processBatch(Batch batch,
                                  @Nullable StringBuilder output,
                                  String sourceName,
                                  String applicationName,
                                  ReportableEntityHandler<Span, String> spanHandler,
                                  ReportableEntityHandler<SpanLogs, String> spanLogsHandler,
                                  @Nullable WavefrontInternalReporter wfInternalReporter,
                                  Supplier<Boolean> traceDisabled,
                                  Supplier<Boolean> spanLogsDisabled,
                                  Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                                  Sampler sampler,
                                  boolean alwaysSampleErrors,
                                  Set<String> traceDerivedCustomTagKeys,
                                  Counter discardedTraces,
                                  Counter discardedBatches,
                                  Counter discardedSpansBySampler,
                                  ConcurrentMap<HeartbeatMetricKey, Boolean> discoveredHeartbeatMetrics) {
    String serviceName = batch.getProcess().getServiceName();
    List<Annotation> processAnnotations = new ArrayList<>();
    boolean isSourceProcessTagPresent = false;
    if (batch.getProcess().getTags() != null) {
      for (Tag tag : batch.getProcess().getTags()) {
        if (tag.getKey().equals(APPLICATION_TAG_KEY) && tag.getVType() == TagType.STRING) {
          applicationName = tag.getVStr();
          continue;
        }

        // source tag precedence :
        // "source" in span tag > "source" in process tag > "hostname" in process tag > DEFAULT
        if (tag.getKey().equals("hostname") && tag.getVType() == TagType.STRING) {
          if (!isSourceProcessTagPresent) {
            sourceName = tag.getVStr();
          }
          continue;
        }

        if (tag.getKey().equals(SOURCE_KEY) && tag.getVType() == TagType.STRING) {
          sourceName = tag.getVStr();
          isSourceProcessTagPresent = true;
          continue;
        }

        //TODO: Propagate other Jaeger process tags as span tags
        if (tag.getKey().equals("ip")) {
          Annotation annotation = tagToAnnotation(tag);
          processAnnotations.add(annotation);
        }
      }
    }
    if (traceDisabled.get()) {
      featureDisabledLogger.info("Ingested spans discarded because tracing feature is not " +
            "enabled on the server");
      discardedBatches.inc();
      discardedTraces.inc(batch.getSpansSize());
      if (output != null) {
        output.append("Ingested spans discarded because tracing feature is not enabled on the " +
            "server.");
      }
      return;
    }
    for (io.jaegertracing.thriftjava.Span span : batch.getSpans()) {
      processSpan(span, serviceName, sourceName, applicationName, processAnnotations,
          spanHandler, spanLogsHandler, wfInternalReporter, spanLogsDisabled,
          preprocessorSupplier, sampler, alwaysSampleErrors, traceDerivedCustomTagKeys,
          discardedSpansBySampler, discoveredHeartbeatMetrics);
    }
  }

  private static void processSpan(io.jaegertracing.thriftjava.Span span,
                                  String serviceName,
                                  String sourceName,
                                  String applicationName,
                                  List<Annotation> processAnnotations,
                                  ReportableEntityHandler<Span, String> spanHandler,
                                  ReportableEntityHandler<SpanLogs, String> spanLogsHandler,
                                  @Nullable WavefrontInternalReporter wfInternalReporter,
                                  Supplier<Boolean> spanLogsDisabled,
                                  Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                                  Sampler sampler,
                                  boolean alwaysSampleErrors,
                                  Set<String> traceDerivedCustomTagKeys,
                                  Counter discardedSpansBySampler,
                                  ConcurrentMap<HeartbeatMetricKey, Boolean> discoveredHeartbeatMetrics) {
    List<Annotation> annotations = new ArrayList<>(processAnnotations);
    // serviceName is mandatory in Jaeger
    annotations.add(new Annotation(SERVICE_TAG_KEY, serviceName));
    long parentSpanId = span.getParentSpanId();
    if (parentSpanId != 0) {
      annotations.add(new Annotation("parent", new UUID(0, parentSpanId).toString()));
    }

    String cluster = NULL_TAG_VAL;
    String shard = NULL_TAG_VAL;
    String componentTagValue = NULL_TAG_VAL;
    boolean isError = false;
    boolean isDebugSpanTag = false;
    boolean isForceSampled = false;

    if (span.getTags() != null) {
      for (Tag tag : span.getTags()) {
        if (IGNORE_TAGS.contains(tag.getKey()) ||
            (tag.vType == TagType.STRING && StringUtils.isBlank(tag.getVStr()))) {
          continue;
        }

        Annotation annotation = tagToAnnotation(tag);
        if (annotation != null) {
          switch (annotation.getKey()) {
            case APPLICATION_TAG_KEY:
              applicationName = annotation.getValue();
              continue;
            case CLUSTER_TAG_KEY:
              cluster = annotation.getValue();
              continue;
            case SHARD_TAG_KEY:
              shard = annotation.getValue();
              continue;
              // Do not add source to annotation span tag list.
            case SOURCE_KEY:
              sourceName = annotation.getValue();
              continue;
            case COMPONENT_TAG_KEY:
              componentTagValue = annotation.getValue();
              break;
            case ERROR_SPAN_TAG_KEY:
              // only error=true is supported
              isError = annotation.getValue().equals(ERROR_SPAN_TAG_VAL);
              break;
            //TODO : Import DEBUG_SPAN_TAG_KEY from wavefront-sdk-java constants.
            case DEBUG_SPAN_TAG_KEY:
              isDebugSpanTag = true;
              break;
            case FORCE_SAMPLED_KEY:
              try {
                if (NumberFormat.getInstance().parse(annotation.getValue()).doubleValue() > 0) {
                  isForceSampled = true;
                }
              } catch (ParseException e) {
                if (JAEGER_DATA_LOGGER.isLoggable(Level.FINE)) {
                  JAEGER_DATA_LOGGER.info("Invalid value :: " + annotation.getValue() +
                      " for span tag key : "+ FORCE_SAMPLED_KEY);
                }
              }
              break;
          }
          annotations.add(annotation);
        }
      }
    }

    // Add all wavefront indexed tags. These are set based on below hierarchy.
    // Span Level > Process Level > Proxy Level > Default
    annotations.add(new Annotation(APPLICATION_TAG_KEY, applicationName));
    annotations.add(new Annotation(CLUSTER_TAG_KEY, cluster));
    annotations.add(new Annotation(SHARD_TAG_KEY, shard));

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
                  new UUID(0, reference.getSpanId()).toString()));
            }
          default:
        }
      }
    }

    if (!spanLogsDisabled.get() && span.getLogs() != null && !span.getLogs().isEmpty()) {
      annotations.add(new Annotation("_spanLogs", "true"));
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

    if (preprocessorSupplier != null) {
      ReportableEntityPreprocessor preprocessor = preprocessorSupplier.get();
      String[] messageHolder = new String[1];
      preprocessor.forSpan().transform(wavefrontSpan);
      if (!preprocessor.forSpan().filter(wavefrontSpan, messageHolder)) {
        if (messageHolder[0] != null) {
          spanHandler.reject(wavefrontSpan, messageHolder[0]);
        } else {
          spanHandler.block(wavefrontSpan);
        }
        return;
      }
    }
    if (isForceSampled || isDebugSpanTag || (alwaysSampleErrors && isError) ||
        sample(wavefrontSpan, sampler, discardedSpansBySampler)) {
      spanHandler.report(wavefrontSpan);
      if (span.getLogs() != null && !span.getLogs().isEmpty()) {
        if (spanLogsDisabled.get()) {
          featureDisabledLogger.info("Span logs discarded because the feature is not " +
              "enabled on the server!");
        } else {
          SpanLogs spanLogs = SpanLogs.newBuilder().
              setCustomer("default").
              setTraceId(wavefrontSpan.getTraceId()).
              setSpanId(wavefrontSpan.getSpanId()).
              setLogs(span.getLogs().stream().map(x -> {
                Map<String, String> fields = new HashMap<>(x.fields.size());
                x.fields.forEach(t -> {
                  switch (t.vType) {
                    case STRING:
                      fields.put(t.getKey(), t.getVStr());
                      break;
                    case BOOL:
                      fields.put(t.getKey(), String.valueOf(t.isVBool()));
                      break;
                    case LONG:
                      fields.put(t.getKey(), String.valueOf(t.getVLong()));
                      break;
                    case DOUBLE:
                      fields.put(t.getKey(), String.valueOf(t.getVDouble()));
                      break;
                    case BINARY:
                      // ignore
                    default:
                  }
                });
                return SpanLog.newBuilder().
                    setTimestamp(x.timestamp).
                    setFields(fields).
                    build();
              }).collect(Collectors.toList())).build();
          spanLogsHandler.report(spanLogs);
        }
      }
    }
    // report stats irrespective of span sampling.
    if (wfInternalReporter != null) {
      // report converted metrics/histograms from the span
      discoveredHeartbeatMetrics.putIfAbsent(reportWavefrontGeneratedData(wfInternalReporter,
          span.getOperationName(), applicationName, serviceName, cluster, shard, sourceName,
          componentTagValue, isError, span.getDuration(), traceDerivedCustomTagKeys,
          annotations), true);
    }
  }

  private static boolean sample(Span wavefrontSpan, Sampler sampler,
                                Counter discardedSpansBySampler) {
    if (sampler.sample(wavefrontSpan.getName(),
        UUID.fromString(wavefrontSpan.getTraceId()).getLeastSignificantBits(),
        wavefrontSpan.getDuration())) {
      return true;
    }
    discardedSpansBySampler.inc();
    return false;
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
