package com.wavefront.agent.listeners.tracing;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;

import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.common.TraceConstants;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.java_sdk.com.google.common.annotations.VisibleForTesting;
import com.wavefront.sdk.common.Pair;
import com.yammer.metrics.core.Counter;

import org.apache.commons.lang.StringUtils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.opentelemetry.exporters.jaeger.proto.api_v2.Model;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

import static com.google.protobuf.util.Durations.toMicros;
import static com.google.protobuf.util.Durations.toMillis;
import static com.google.protobuf.util.Timestamps.toMicros;
import static com.google.protobuf.util.Timestamps.toMillis;
import static com.wavefront.agent.listeners.FeatureCheckUtils.SPANLOGS_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.SPAN_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.isFeatureDisabled;
import static com.wavefront.internal.SpanDerivedMetricsUtils.DEBUG_SPAN_TAG_VAL;
import static com.wavefront.internal.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;
import static com.wavefront.internal.SpanDerivedMetricsUtils.reportWavefrontGeneratedData;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.COMPONENT_TAG_KEY;
import static com.wavefront.sdk.common.Constants.DEBUG_TAG_KEY;
import static com.wavefront.sdk.common.Constants.ERROR_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SOURCE_KEY;

/**
 * Utility methods for processing Jaeger Protobuf trace data.
 *
 * @author Hao Song (songhao@vmware.com)
 */
public abstract class JaegerProtobufUtils {
  protected static final Logger logger =
      Logger.getLogger(JaegerProtobufUtils.class.getCanonicalName());

  // TODO: support sampling
  private final static Set<String> IGNORE_TAGS = ImmutableSet.of("sampler.type", "sampler.param");
  private final static String FORCE_SAMPLED_KEY = "sampling.priority";
  private static final Logger JAEGER_DATA_LOGGER = Logger.getLogger("JaegerDataLogger");

  private JaegerProtobufUtils() {
  }

  public static void processBatch(Model.Batch batch,
                                  @Nullable StringBuilder output,
                                  String sourceName,
                                  String applicationName,
                                  ReportableEntityHandler<Span, String> spanHandler,
                                  ReportableEntityHandler<SpanLogs, String> spanLogsHandler,
                                  @Nullable WavefrontInternalReporter wfInternalReporter,
                                  Supplier<Boolean> traceDisabled,
                                  Supplier<Boolean> spanLogsDisabled,
                                  Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                                  SpanSampler sampler,
                                  Set<String> traceDerivedCustomTagKeys,
                                  Counter discardedTraces,
                                  Counter discardedBatches,
                                  Counter discardedSpansBySampler,
                                  Set<Pair<Map<String, String>, String>> discoveredHeartbeatMetrics) {
    String serviceName = batch.getProcess().getServiceName();
    List<Annotation> processAnnotations = new ArrayList<>();
    boolean isSourceProcessTagPresent = false;
    if (batch.getProcess().getTagsList() != null) {
      for (Model.KeyValue tag : batch.getProcess().getTagsList()) {
        if (tag.getKey().equals(APPLICATION_TAG_KEY) && tag.getVType() == Model.ValueType.STRING) {
          applicationName = tag.getVStr();
          continue;
        }

        // source tag precedence :
        // "source" in span tag > "source" in process tag > "hostname" in process tag > DEFAULT
        if (tag.getKey().equals("hostname") && tag.getVType() == Model.ValueType.STRING) {
          if (!isSourceProcessTagPresent) {
            sourceName = tag.getVStr();
          }
          continue;
        }

        if (tag.getKey().equals(SOURCE_KEY) && tag.getVType() == Model.ValueType.STRING) {
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
    if (isFeatureDisabled(traceDisabled, SPAN_DISABLED, discardedBatches, output)) {
      discardedTraces.inc(batch.getSpansCount());
      return;
    }
    for (Model.Span span : batch.getSpansList()) {
      processSpan(span, serviceName, sourceName, applicationName, processAnnotations,
          spanHandler, spanLogsHandler, wfInternalReporter, spanLogsDisabled,
          preprocessorSupplier, sampler, traceDerivedCustomTagKeys,
          discardedSpansBySampler, discoveredHeartbeatMetrics);
    }
  }

  private static void processSpan(Model.Span span,
                                  String serviceName,
                                  String sourceName,
                                  String applicationName,
                                  List<Annotation> processAnnotations,
                                  ReportableEntityHandler<Span, String> spanHandler,
                                  ReportableEntityHandler<SpanLogs, String> spanLogsHandler,
                                  @Nullable WavefrontInternalReporter wfInternalReporter,
                                  Supplier<Boolean> spanLogsDisabled,
                                  Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                                  SpanSampler sampler,
                                  Set<String> traceDerivedCustomTagKeys,
                                  Counter discardedSpansBySampler,
                                  Set<Pair<Map<String, String>, String>> discoveredHeartbeatMetrics) {
    List<Annotation> annotations = new ArrayList<>(processAnnotations);
    // serviceName is mandatory in Jaeger
    annotations.add(new Annotation(SERVICE_TAG_KEY, serviceName));

    String cluster = NULL_TAG_VAL;
    String shard = NULL_TAG_VAL;
    String componentTagValue = NULL_TAG_VAL;
    boolean isError = false;
    boolean isDebugSpanTag = false;
    boolean isForceSampled = false;

    if (span.getTagsList() != null) {
      for (Model.KeyValue tag : span.getTagsList()) {
        if (IGNORE_TAGS.contains(tag.getKey()) ||
            (tag.getVType() == Model.ValueType.STRING && StringUtils.isBlank(tag.getVStr()))) {
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
            case ERROR_TAG_KEY:
              // only error=true is supported
              isError = annotation.getValue().equals(ERROR_SPAN_TAG_VAL);
              break;
            case DEBUG_TAG_KEY:
              isDebugSpanTag = annotation.getValue().equals(DEBUG_SPAN_TAG_VAL);
              break;
            case FORCE_SAMPLED_KEY:
              try {
                if (NumberFormat.getInstance().parse(annotation.getValue()).doubleValue() > 0) {
                  isForceSampled = true;
                }
              } catch (ParseException e) {
                if (JAEGER_DATA_LOGGER.isLoggable(Level.FINE)) {
                  JAEGER_DATA_LOGGER.info("Invalid value :: " + annotation.getValue() +
                      " for span tag key : " + FORCE_SAMPLED_KEY + " for span : " + span.getOperationName());
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

    if (span.getReferencesList() != null) {
      for (Model.SpanRef reference : span.getReferencesList()) {
        switch (reference.getRefType()) {
          case CHILD_OF:
            if (!reference.getSpanId().isEmpty()) {
              annotations.add(new Annotation(TraceConstants.PARENT_KEY,
                  toStringId(reference.getSpanId())));
            }
            break;
          case FOLLOWS_FROM:
            if (!reference.getSpanId().isEmpty()) {
              annotations.add(new Annotation(TraceConstants.FOLLOWS_FROM_KEY,
                  toStringId(reference.getSpanId())));
            }
          default:
        }
      }
    }

    if (!spanLogsDisabled.get() && span.getLogsCount() > 0) {
      annotations.add(new Annotation("_spanLogs", "true"));
    }

    Span wavefrontSpan = Span.newBuilder()
        .setCustomer("dummy")
        .setName(span.getOperationName())
        .setSource(sourceName)
        .setSpanId(toStringId(span.getSpanId()))
        .setTraceId(toStringId(span.getTraceId()))
        .setStartMillis(toMillis(span.getStartTime()))
        .setDuration(toMillis(span.getDuration()))
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
    if (isForceSampled || sampler.sample(wavefrontSpan, discardedSpansBySampler)) {
      spanHandler.report(wavefrontSpan);
      if (span.getLogsCount() > 0 &&
          !isFeatureDisabled(spanLogsDisabled, SPANLOGS_DISABLED, null)) {
        SpanLogs spanLogs = SpanLogs.newBuilder().
            setCustomer("default").
            setTraceId(wavefrontSpan.getTraceId()).
            setSpanId(wavefrontSpan.getSpanId()).
            setLogs(span.getLogsList().stream().map(x -> {
              Map<String, String> fields = new HashMap<>(x.getFieldsCount());
              x.getFieldsList().forEach(t -> {
                switch (t.getVType()) {
                  case STRING:
                    fields.put(t.getKey(), t.getVStr());
                    break;
                  case BOOL:
                    fields.put(t.getKey(), String.valueOf(t.getVBool()));
                    break;
                  case INT64:
                    fields.put(t.getKey(), String.valueOf(t.getVInt64()));
                    break;
                  case FLOAT64:
                    fields.put(t.getKey(), String.valueOf(t.getVFloat64()));
                    break;
                  case BINARY:
                    // ignore
                  default:
                }
              });
              return SpanLog.newBuilder().
                  setTimestamp(toMicros(x.getTimestamp())).
                  setFields(fields).
                  build();
            }).collect(Collectors.toList())).build();
        spanLogsHandler.report(spanLogs);
      }
    }

    // report stats irrespective of span sampling.
    if (wfInternalReporter != null) {
      // Set post preprocessor rule values and report converted metrics/histograms from the span
      List<Annotation> processedAnnotations = wavefrontSpan.getAnnotations();
      for (Annotation processedAnnotation : processedAnnotations) {
        switch (processedAnnotation.getKey()) {
          case APPLICATION_TAG_KEY:
            applicationName = processedAnnotation.getValue();
            continue;
          case SERVICE_TAG_KEY:
            serviceName = processedAnnotation.getValue();
            continue;
          case CLUSTER_TAG_KEY:
            cluster = processedAnnotation.getValue();
            continue;
          case SHARD_TAG_KEY:
            shard = processedAnnotation.getValue();
            continue;
          case COMPONENT_TAG_KEY:
            componentTagValue = processedAnnotation.getValue();
            continue;
          case ERROR_TAG_KEY:
            isError = processedAnnotation.getValue().equals(ERROR_SPAN_TAG_VAL);
            continue;
        }
      }
      List<Pair<String, String>> spanTags = processedAnnotations.stream().map(a -> new Pair<>(a.getKey(),
          a.getValue())).collect(Collectors.toList());
      discoveredHeartbeatMetrics.add(reportWavefrontGeneratedData(wfInternalReporter,
          wavefrontSpan.getName(), applicationName, serviceName, cluster, shard, wavefrontSpan.getSource(),
          componentTagValue, isError, toMicros(span.getDuration()), traceDerivedCustomTagKeys,
          spanTags, true));
    }
  }

  @VisibleForTesting
  protected static String toStringId(ByteString id) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(id.toByteArray());
    long mostSigBits = id.toByteArray().length > 8 ? byteBuffer.getLong() : 0L;
    long leastSigBits = new BigInteger(1, byteBuffer.array()).longValue();
    UUID uuid = new UUID(mostSigBits, leastSigBits);
    return uuid.toString();
  }

  @Nullable
  private static Annotation tagToAnnotation(Model.KeyValue tag) {
    switch (tag.getVType()) {
      case BOOL:
        return new Annotation(tag.getKey(), String.valueOf(tag.getVBool()));
      case INT64:
        return new Annotation(tag.getKey(), String.valueOf(tag.getVInt64()));
      case FLOAT64:
        return new Annotation(tag.getKey(), String.valueOf(tag.getVFloat64()));
      case STRING:
        return new Annotation(tag.getKey(), tag.getVStr());
      case BINARY:
      default:
        return null;
    }
  }
}
