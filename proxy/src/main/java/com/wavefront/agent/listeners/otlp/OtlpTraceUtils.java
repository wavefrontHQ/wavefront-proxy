package com.wavefront.agent.listeners.otlp;

import static com.wavefront.agent.listeners.FeatureCheckUtils.SPANLOGS_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.isFeatureDisabled;
import static com.wavefront.common.TraceConstants.PARENT_KEY;
import static com.wavefront.internal.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;
import static com.wavefront.internal.SpanDerivedMetricsUtils.TRACING_DERIVED_PREFIX;
import static com.wavefront.internal.SpanDerivedMetricsUtils.reportWavefrontGeneratedData;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.COMPONENT_TAG_KEY;
import static com.wavefront.sdk.common.Constants.ERROR_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SOURCE_KEY;
import static com.wavefront.sdk.common.Constants.SPAN_LOG_KEY;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.wavefront.agent.core.handlers.ReportableEntityHandler;
import com.wavefront.agent.listeners.tracing.SpanUtils;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.common.WavefrontSender;
import com.yammer.metrics.core.Counter;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span.SpanKind;
import io.opentelemetry.proto.trace.v1.Status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

public class OtlpTraceUtils {
  // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk_exporters/non-otlp.md#span-status
  public static final String OTEL_DROPPED_ATTRS_KEY = "otel.dropped_attributes_count";
  public static final String OTEL_DROPPED_EVENTS_KEY = "otel.dropped_events_count";
  public static final String OTEL_DROPPED_LINKS_KEY = "otel.dropped_links_count";
  public static final String OTEL_SERVICE_NAME_KEY = "service.name";
  public static final String OTEL_STATUS_DESCRIPTION_KEY = "otel.status_description";
  private static final String DEFAULT_APPLICATION_NAME = "defaultApplication";
  private static final String DEFAULT_SERVICE_NAME = "defaultService";
  private static final Logger OTLP_DATA_LOGGER = Logger.getLogger("OTLPDataLogger");
  private static final String SPAN_EVENT_TAG_KEY = "name";
  private static final String SPAN_KIND_TAG_KEY = "span.kind";
  private static final HashMap<SpanKind, Annotation> SPAN_KIND_ANNOTATION_HASH_MAP =
          new HashMap<SpanKind, Annotation>() {
            {
              put(SpanKind.SPAN_KIND_CLIENT, new Annotation(SPAN_KIND_TAG_KEY, "client"));
              put(SpanKind.SPAN_KIND_CONSUMER, new Annotation(SPAN_KIND_TAG_KEY, "consumer"));
              put(SpanKind.SPAN_KIND_INTERNAL, new Annotation(SPAN_KIND_TAG_KEY, "internal"));
              put(SpanKind.SPAN_KIND_PRODUCER, new Annotation(SPAN_KIND_TAG_KEY, "producer"));
              put(SpanKind.SPAN_KIND_SERVER, new Annotation(SPAN_KIND_TAG_KEY, "server"));
              put(SpanKind.SPAN_KIND_UNSPECIFIED, new Annotation(SPAN_KIND_TAG_KEY, "unspecified"));
              put(SpanKind.UNRECOGNIZED, new Annotation(SPAN_KIND_TAG_KEY, "unknown"));
            }
          };

  public static KeyValue getAttrByKey(List<KeyValue> attributesList, String key) {
    return attributesList.stream().filter(kv -> key.equals(kv.getKey())).findFirst().orElse(null);
  }

  public static KeyValue buildKeyValue(String key, String value) {
    return KeyValue.newBuilder()
            .setKey(key)
            .setValue(AnyValue.newBuilder().setStringValue(value).build())
            .build();
  }

  static class WavefrontSpanAndLogs {
    Span span;
    SpanLogs spanLogs;

    public WavefrontSpanAndLogs(Span span, SpanLogs spanLogs) {
      this.span = span;
      this.spanLogs = spanLogs;
    }

    public Span getSpan() {
      return span;
    }

    public SpanLogs getSpanLogs() {
      return spanLogs;
    }
  }

  public static void exportToWavefront(
      ExportTraceServiceRequest request,
      ReportableEntityHandler<Span> spanHandler,
      ReportableEntityHandler<SpanLogs> spanLogsHandler,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      Pair<Supplier<Boolean>, Counter> spanLogsDisabled,
      Pair<SpanSampler, Counter> samplerAndCounter,
      String defaultSource,
      Set<Pair<Map<String, String>, String>> discoveredHeartbeatMetrics,
      WavefrontInternalReporter internalReporter,
      Set<String> traceDerivedCustomTagKeys) {
    ReportableEntityPreprocessor preprocessor = null;
    if (preprocessorSupplier != null) {
      preprocessor = preprocessorSupplier.get();
    }

    for (WavefrontSpanAndLogs spanAndLogs : fromOtlpRequest(request, preprocessor, defaultSource)) {
      Span span = spanAndLogs.getSpan();
      SpanLogs spanLogs = spanAndLogs.getSpanLogs();

      if (wasFilteredByPreprocessor(span, spanHandler, preprocessor)) continue;

      if (samplerAndCounter._1.sample(span, samplerAndCounter._2)) {
        spanHandler.report(span);

        if (shouldReportSpanLogs(spanLogs.getLogs().size(), spanLogsDisabled)) {
          SpanUtils.addSpanLine(span, spanLogs);
          spanLogsHandler.report(spanLogs);
        }
      }

      // always report RED metrics irrespective of span sampling
      discoveredHeartbeatMetrics.add(
              reportREDMetrics(span, internalReporter, traceDerivedCustomTagKeys));
    }
  }

  // TODO: consider transforming a single span and returning it for immedidate reporting in
  //   wfSender. This could be more efficient, and also more reliable in the event the loops
  //   below throw an error and we don't report any of the list.
  @VisibleForTesting
  static List<WavefrontSpanAndLogs> fromOtlpRequest(
          ExportTraceServiceRequest request,
          @Nullable ReportableEntityPreprocessor preprocessor,
          String defaultSource) {
    List<WavefrontSpanAndLogs> wfSpansAndLogs = new ArrayList<>();

    for (ResourceSpans rSpans : request.getResourceSpansList()) {
      Resource resource = rSpans.getResource();
      OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Resource: " + resource);

      for (ScopeSpans scopeSpans : rSpans.getScopeSpansList()) {
        InstrumentationScope scope = scopeSpans.getScope();
        OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Instrumentation Scope: " + scope);

        for (io.opentelemetry.proto.trace.v1.Span otlpSpan : scopeSpans.getSpansList()) {
          OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Span: " + otlpSpan);

          wfSpansAndLogs.add(
                  transformAll(
                          otlpSpan, resource.getAttributesList(), scope, preprocessor, defaultSource));
        }
      }
    }
    return wfSpansAndLogs;
  }

  @VisibleForTesting
  static boolean wasFilteredByPreprocessor(
      Span wfSpan,
      ReportableEntityHandler<Span> spanHandler,
      @Nullable ReportableEntityPreprocessor preprocessor) {
    if (preprocessor == null) {
      return false;
    }

    String[] messageHolder = new String[1];
    if (!preprocessor.forSpan().filter(wfSpan, messageHolder)) {
      if (messageHolder[0] != null) {
        spanHandler.reject(wfSpan, messageHolder[0]);
      } else {
        spanHandler.block(wfSpan);
      }
      return true;
    }

    return false;
  }

  @VisibleForTesting
  static WavefrontSpanAndLogs transformAll(
          io.opentelemetry.proto.trace.v1.Span otlpSpan,
          List<KeyValue> resourceAttributes,
          InstrumentationScope scope,
          @Nullable ReportableEntityPreprocessor preprocessor,
          String defaultSource) {
    Span span = transformSpan(otlpSpan, resourceAttributes, scope, preprocessor, defaultSource);
    SpanLogs logs = transformEvents(otlpSpan, span);
    if (!logs.getLogs().isEmpty()) {
      span.getAnnotations().add(new Annotation(SPAN_LOG_KEY, "true"));
    }

    OTLP_DATA_LOGGER.finest(() -> "Converted Wavefront Span: " + span);
    if (!logs.getLogs().isEmpty()) {
      OTLP_DATA_LOGGER.finest(() -> "Converted Wavefront SpanLogs: " + logs);
    }

    return new WavefrontSpanAndLogs(span, logs);
  }

  @VisibleForTesting
  static Span transformSpan(
          io.opentelemetry.proto.trace.v1.Span otlpSpan,
          List<KeyValue> resourceAttrs,
          InstrumentationScope scope,
          ReportableEntityPreprocessor preprocessor,
          String defaultSource) {
    Pair<String, List<KeyValue>> sourceAndResourceAttrs =
            sourceFromAttributes(resourceAttrs, defaultSource);
    String source = sourceAndResourceAttrs._1;
    resourceAttrs = sourceAndResourceAttrs._2;

    // Order of arguments to Stream.of() matters: when a Resource Attribute and a Span Attribute
    // happen to share the same key, we want the Span Attribute to "win" and be preserved.
    List<KeyValue> otlpAttributes =
            Stream.of(resourceAttrs, otlpSpan.getAttributesList())
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

    List<Annotation> wfAnnotations = annotationsFromAttributes(otlpAttributes);

    wfAnnotations.add(SPAN_KIND_ANNOTATION_HASH_MAP.get(otlpSpan.getKind()));
    wfAnnotations.addAll(annotationsFromStatus(otlpSpan.getStatus()));
    wfAnnotations.addAll(annotationsFromInstrumentationScope(scope));
    wfAnnotations.addAll(annotationsFromDroppedCounts(otlpSpan));
    wfAnnotations.addAll(annotationsFromTraceState(otlpSpan.getTraceState()));
    wfAnnotations.addAll(annotationsFromParentSpanID(otlpSpan.getParentSpanId()));

    String wfSpanId = SpanUtils.toStringId(otlpSpan.getSpanId());
    String wfTraceId = SpanUtils.toStringId(otlpSpan.getTraceId());
    long startTimeMs = TimeUnit.NANOSECONDS.toMillis(otlpSpan.getStartTimeUnixNano());
    long durationMs =
            otlpSpan.getEndTimeUnixNano() == 0
                    ? 0
                    : TimeUnit.NANOSECONDS.toMillis(
                    otlpSpan.getEndTimeUnixNano() - otlpSpan.getStartTimeUnixNano());

    wavefront.report.Span toReturn =
            wavefront.report.Span.newBuilder()
                    .setName(otlpSpan.getName())
                    .setSpanId(wfSpanId)
                    .setTraceId(wfTraceId)
                    .setStartMillis(startTimeMs)
                    .setDuration(durationMs)
                    .setAnnotations(wfAnnotations)
                    .setSource(source)
                    .setCustomer("dummy")
                    .build();

    // apply preprocessor
    if (preprocessor != null) {
      preprocessor.forSpan().transform(toReturn);
    }

    // After preprocessor has run `transform()`, set required WF tags that may still be missing
    List<Annotation> processedAnnotationList = setRequiredTags(toReturn.getAnnotations());
    toReturn.setAnnotations(processedAnnotationList);
    return toReturn;
  }

  @VisibleForTesting
  static SpanLogs transformEvents(io.opentelemetry.proto.trace.v1.Span otlpSpan, Span wfSpan) {
    ArrayList<SpanLog> logs = new ArrayList<>();

    for (io.opentelemetry.proto.trace.v1.Span.Event event : otlpSpan.getEventsList()) {
      SpanLog log = new SpanLog();
      log.setTimestamp(TimeUnit.NANOSECONDS.toMicros(event.getTimeUnixNano()));
      Map<String, String> fields = mapFromAttributes(event.getAttributesList());
      fields.put(SPAN_EVENT_TAG_KEY, event.getName());
      if (event.getDroppedAttributesCount() != 0) {
        fields.put(OTEL_DROPPED_ATTRS_KEY, String.valueOf(event.getDroppedAttributesCount()));
      }
      log.setFields(fields);
      logs.add(log);
    }

    return SpanLogs.newBuilder()
            .setLogs(logs)
            .setSpanId(wfSpan.getSpanId())
            .setTraceId(wfSpan.getTraceId())
            .setCustomer(wfSpan.getCustomer())
            .build();
  }

  // Returns a String of the source value and the original List<KeyValue> attributes except
  // with the removal of the KeyValue determined to be the source.
  @VisibleForTesting
  static Pair<String, List<KeyValue>> sourceFromAttributes(
          List<KeyValue> otlpAttributes, String defaultSource) {
    // Order of keys in List matters: it determines precedence when multiple candidates exist.
    List<String> candidateKeys = Arrays.asList(SOURCE_KEY, "host.name", "hostname", "host.id");
    Comparator<KeyValue> keySorter = Comparator.comparing(kv -> candidateKeys.indexOf(kv.getKey()));

    Optional<KeyValue> sourceAttr =
            otlpAttributes.stream()
                    .filter(kv -> candidateKeys.contains(kv.getKey()))
                    .sorted(keySorter)
                    .findFirst();

    if (sourceAttr.isPresent()) {
      List<KeyValue> attributesWithoutSource = new ArrayList<>(otlpAttributes);
      attributesWithoutSource.remove(sourceAttr.get());

      return new Pair<>(fromAnyValue(sourceAttr.get().getValue()), attributesWithoutSource);
    } else {
      return new Pair<>(defaultSource, otlpAttributes);
    }
  }

  @VisibleForTesting
  static List<Annotation> annotationsFromAttributes(List<KeyValue> attributesList) {
    List<Annotation> annotations = new ArrayList<>();
    for (KeyValue attribute : attributesList) {
      String key = attribute.getKey().equals(SOURCE_KEY) ? "_source" : attribute.getKey();
      Annotation.Builder annotationBuilder = Annotation.newBuilder().setKey(key);

      if (!attribute.hasValue()) {
        annotationBuilder.setValue("");
      } else {
        annotationBuilder.setValue(fromAnyValue(attribute.getValue()));
      }
      annotations.add(annotationBuilder.build());
    }

    return annotations;
  }

  @VisibleForTesting
  static List<Annotation> annotationsFromInstrumentationScope(InstrumentationScope scope) {
    if (scope == null || scope.getName().isEmpty()) return Collections.emptyList();

    List<Annotation> annotations = new ArrayList<>();

    // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk_exporters/non-otlp.md
    annotations.add(new Annotation("otel.scope.name", scope.getName()));
    if (!scope.getVersion().isEmpty()) {
      annotations.add(new Annotation("otel.scope.version", scope.getVersion()));
    }

    return annotations;
  }

  @VisibleForTesting
  static List<Annotation> annotationsFromDroppedCounts(
          io.opentelemetry.proto.trace.v1.Span otlpSpan) {
    List<Annotation> annotations = new ArrayList<>();
    if (otlpSpan.getDroppedAttributesCount() != 0) {
      annotations.add(
              new Annotation(
                      OTEL_DROPPED_ATTRS_KEY, String.valueOf(otlpSpan.getDroppedAttributesCount())));
    }
    if (otlpSpan.getDroppedEventsCount() != 0) {
      annotations.add(
              new Annotation(
                      OTEL_DROPPED_EVENTS_KEY, String.valueOf(otlpSpan.getDroppedEventsCount())));
    }
    if (otlpSpan.getDroppedLinksCount() != 0) {
      annotations.add(
              new Annotation(OTEL_DROPPED_LINKS_KEY, String.valueOf(otlpSpan.getDroppedLinksCount())));
    }

    return annotations;
  }

  @VisibleForTesting
  static Pair<Map<String, String>, String> reportREDMetrics(
          Span span,
          WavefrontInternalReporter internalReporter,
          Set<String> traceDerivedCustomTagKeys) {
    Map<String, String> annotations = mapFromAnnotations(span.getAnnotations());
    List<Pair<String, String>> spanTags =
            span.getAnnotations().stream()
                    .map(a -> Pair.of(a.getKey(), a.getValue()))
                    .collect(Collectors.toList());

    return reportWavefrontGeneratedData(
            internalReporter,
            span.getName(),
            annotations.get(APPLICATION_TAG_KEY),
            annotations.get(SERVICE_TAG_KEY),
            annotations.get(CLUSTER_TAG_KEY),
            annotations.get(SHARD_TAG_KEY),
            span.getSource(),
            annotations.getOrDefault(COMPONENT_TAG_KEY, NULL_TAG_VAL),
            Boolean.parseBoolean(annotations.get(ERROR_TAG_KEY)),
            TimeUnit.MILLISECONDS.toMicros(span.getDuration()),
            traceDerivedCustomTagKeys,
            spanTags);
  }

  @VisibleForTesting
  static List<Annotation> setRequiredTags(List<Annotation> annotationList) {
    Map<String, String> tags = mapFromAnnotations(annotationList);
    List<Annotation> requiredTags = new ArrayList<>();

    if (!tags.containsKey(SERVICE_TAG_KEY)) {
      tags.put(SERVICE_TAG_KEY, tags.getOrDefault(OTEL_SERVICE_NAME_KEY, DEFAULT_SERVICE_NAME));
    }
    tags.remove(OTEL_SERVICE_NAME_KEY);

    tags.putIfAbsent(APPLICATION_TAG_KEY, DEFAULT_APPLICATION_NAME);
    tags.putIfAbsent(CLUSTER_TAG_KEY, NULL_TAG_VAL);
    tags.putIfAbsent(SHARD_TAG_KEY, NULL_TAG_VAL);

    for (Map.Entry<String, String> tagEntry : tags.entrySet()) {
      requiredTags.add(
              Annotation.newBuilder().setKey(tagEntry.getKey()).setValue(tagEntry.getValue()).build());
    }

    return requiredTags;
  }

  static long getSpansCount(ExportTraceServiceRequest request) {
    return request.getResourceSpansList().stream()
            .flatMapToLong(r -> r.getScopeSpansList().stream().mapToLong(ScopeSpans::getSpansCount))
            .sum();
  }

  @VisibleForTesting
  static boolean shouldReportSpanLogs(
          int logsCount, Pair<Supplier<Boolean>, Counter> spanLogsDisabled) {
    return logsCount > 0
            && !isFeatureDisabled(
            spanLogsDisabled._1, SPANLOGS_DISABLED, spanLogsDisabled._2, logsCount);
  }

  @Nullable
  static WavefrontInternalReporter createAndStartInternalReporter(
          @Nullable WavefrontSender sender) {
    if (sender == null) return null;

    /*
    Internal reporter should have a custom source identifying where the internal metrics came from.
    This mirrors the behavior in the Custom Tracing Listener and Jaeger Listeners.
     */
    WavefrontInternalReporter reporter =
            new WavefrontInternalReporter.Builder()
                    .prefixedWith(TRACING_DERIVED_PREFIX)
                    .withSource("otlp")
                    .reportMinuteDistribution()
                    .build(sender);
    reporter.start(1, TimeUnit.MINUTES);
    return reporter;
  }

  /**
   * Converts an OTLP AnyValue object to its equivalent String representation. The implementation
   * mimics {@code AsString()} from the OpenTelemetry Collector:
   * https://github.com/open-telemetry/opentelemetry-collector/blob/cffbecb2ac9ee98e6a60d22f910760be48a94c55/model/pdata/common.go#L384
   *
   * <p>We do not handle {@code KvlistValue} because the OpenTelemetry Specification for Attributes
   * does not include maps as an allowed type of value:
   * https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/common/common.md#attributes
   *
   * @param anyValue OTLP Attributes value in {@link AnyValue} format
   * @return String representation of the {@link AnyValue}
   */
  static String fromAnyValue(AnyValue anyValue) {
    if (anyValue.hasStringValue()) {
      return anyValue.getStringValue();
    } else if (anyValue.hasBoolValue()) {
      return Boolean.toString(anyValue.getBoolValue());
    } else if (anyValue.hasIntValue()) {
      return Long.toString(anyValue.getIntValue());
    } else if (anyValue.hasDoubleValue()) {
      return Double.toString(anyValue.getDoubleValue());
    } else if (anyValue.hasArrayValue()) {
      List<AnyValue> values = anyValue.getArrayValue().getValuesList();
      return values.stream()
              .map(OtlpTraceUtils::fromAnyValue)
              .collect(Collectors.joining(", ", "[", "]"));
    } else if (anyValue.hasKvlistValue()) {
      OTLP_DATA_LOGGER.finest(() -> "Encountered KvlistValue but cannot convert to String");
    } else if (anyValue.hasBytesValue()) {
      return Base64.getEncoder().encodeToString(anyValue.getBytesValue().toByteArray());
    }
    return "<Unknown OpenTelemetry attribute value type " + anyValue.getValueCase() + ">";
  }

  static Map<String, String> mapFromAttributes(List<KeyValue> attributes) {
    Map<String, String> map = new HashMap<>();
    for (KeyValue attribute : attributes) {
      map.put(attribute.getKey(), fromAnyValue(attribute.getValue()));
    }
    return map;
  }

  private static Map<String, String> mapFromAnnotations(List<Annotation> annotations) {
    Map<String, String> map = new HashMap<>();
    for (Annotation annotation : annotations) {
      map.put(annotation.getKey(), annotation.getValue());
    }
    return map;
  }

  private static List<Annotation> annotationsFromStatus(Status otlpStatus) {
    if (otlpStatus.getCode() != Status.StatusCode.STATUS_CODE_ERROR) return Collections.emptyList();

    List<Annotation> statusAnnotations = new ArrayList<>();
    statusAnnotations.add(new Annotation(ERROR_TAG_KEY, ERROR_SPAN_TAG_VAL));

    if (!otlpStatus.getMessage().isEmpty()) {
      statusAnnotations.add(new Annotation(OTEL_STATUS_DESCRIPTION_KEY, otlpStatus.getMessage()));
    }
    return statusAnnotations;
  }

  private static List<Annotation> annotationsFromTraceState(String state) {
    if (state == null || state.isEmpty()) return Collections.emptyList();

    return Collections.singletonList(new Annotation("w3c.tracestate", state));
  }

  private static List<Annotation> annotationsFromParentSpanID(ByteString parentSpanId) {
    if (parentSpanId == null || parentSpanId.equals(ByteString.EMPTY))
      return Collections.emptyList();

    return Collections.singletonList(
            new Annotation(PARENT_KEY, SpanUtils.toStringId(parentSpanId)));
  }
}