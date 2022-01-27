package com.wavefront.agent.listeners.otlp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.listeners.tracing.SpanUtils;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.Pair;

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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span.SpanKind;
import io.opentelemetry.proto.trace.v1.Status;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

import static com.wavefront.common.TraceConstants.PARENT_KEY;
import static com.wavefront.internal.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.ERROR_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SOURCE_KEY;
import static com.wavefront.sdk.common.Constants.SPAN_LOG_KEY;
import static io.opentelemetry.semconv.resource.attributes.ResourceAttributes.SERVICE_NAME;

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
public class OtlpProtobufUtils {
  // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk_exporters/non-otlp.md#span-status
  public final static String OTEL_STATUS_DESCRIPTION_KEY = "otel.status_description";
  private final static String DEFAULT_APPLICATION_NAME = "defaultApplication";
  private final static String DEFAULT_SERVICE_NAME = "defaultService";
  private final static Logger OTLP_DATA_LOGGER = Logger.getLogger("OTLPDataLogger");
  private final static String SPAN_EVENT_TAG_KEY = "name";
  private final static String SPAN_KIND_TAG_KEY = "span.kind";
  private final static HashMap<SpanKind, Annotation> SPAN_KIND_ANNOTATION_HASH_MAP =
      new HashMap<SpanKind, Annotation>() {{
        put(SpanKind.SPAN_KIND_CLIENT, new Annotation(SPAN_KIND_TAG_KEY, "client"));
        put(SpanKind.SPAN_KIND_CONSUMER, new Annotation(SPAN_KIND_TAG_KEY, "consumer"));
        put(SpanKind.SPAN_KIND_INTERNAL, new Annotation(SPAN_KIND_TAG_KEY, "internal"));
        put(SpanKind.SPAN_KIND_PRODUCER, new Annotation(SPAN_KIND_TAG_KEY, "producer"));
        put(SpanKind.SPAN_KIND_SERVER, new Annotation(SPAN_KIND_TAG_KEY, "server"));
        put(SpanKind.SPAN_KIND_UNSPECIFIED, new Annotation(SPAN_KIND_TAG_KEY, "unspecified"));
        put(SpanKind.UNRECOGNIZED, new Annotation(SPAN_KIND_TAG_KEY, "unknown"));
      }};

  public static void exportToWavefront(ExportTraceServiceRequest request,
                                       ReportableEntityHandler<Span, String> spanHandler,
                                       ReportableEntityHandler<SpanLogs, String> spanLogsHandler,
                                       @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                                       String defaultSource) {
    ReportableEntityPreprocessor preprocessor = null;
    if (preprocessorSupplier != null) {
      preprocessor = preprocessorSupplier.get();
    }

    for (Pair<Span, SpanLogs> wfSpanAndLogs : fromOtlpRequest(request, preprocessor,
        defaultSource)) {
      // TODO: handle sampler
      // TODO: commenting out `wasFilteredByPreprocessor` check doesn't fail any tests
      if (!wasFilteredByPreprocessor(wfSpanAndLogs._1, spanHandler, preprocessor)) {
        spanHandler.report(wfSpanAndLogs._1);
        if (!wfSpanAndLogs._2.getLogs().isEmpty()) {
          spanLogsHandler.report(wfSpanAndLogs._2);
        }
      }
    }
  }

  // TODO: consider transforming a single span and returning it for immedidate reporting in
  //   wfSender. This could be more efficient, and also more reliable in the event the loops
  //   below throw an error and we don't report any of the list.
  private static List<Pair<Span, SpanLogs>> fromOtlpRequest(
      ExportTraceServiceRequest request,
      @Nullable ReportableEntityPreprocessor preprocessor,
      String defaultSource
  ) {
    List<Pair<Span, SpanLogs>> wfSpansAndLogs = Lists.newArrayList();

    for (ResourceSpans rSpans : request.getResourceSpansList()) {
      Resource resource = rSpans.getResource();
      OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Resource: " + resource);

      for (InstrumentationLibrarySpans ilSpans : rSpans.getInstrumentationLibrarySpansList()) {
        OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Instrumentation Library: " +
            ilSpans.getInstrumentationLibrary());

        for (io.opentelemetry.proto.trace.v1.Span otlpSpan : ilSpans.getSpansList()) {
          OTLP_DATA_LOGGER.finest(() -> "Inbound OTLP Span: " + otlpSpan);

          Pair<Span, SpanLogs> pair =
              transformAll(otlpSpan, resource.getAttributesList(), preprocessor, defaultSource);
          OTLP_DATA_LOGGER.finest(() -> "Converted Wavefront Span: " + pair._1);
          if (!pair._2.getLogs().isEmpty()) {
            OTLP_DATA_LOGGER.finest(() -> "Converted Wavefront SpanLogs: " + pair._2);
          }

          wfSpansAndLogs.add(pair);
        }
      }
    }
    return wfSpansAndLogs;
  }

  @VisibleForTesting
  static boolean wasFilteredByPreprocessor(Span wfSpan,
                                           ReportableEntityHandler<Span, String> spanHandler,
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

  public static Pair<Span, SpanLogs> transformAll(io.opentelemetry.proto.trace.v1.Span otlpSpan,
                                                  List<KeyValue> resourceAttributes,
                                                  @Nullable ReportableEntityPreprocessor preprocessor,
                                                  String defaultSource) {
    Span span = transformSpan(otlpSpan, resourceAttributes, preprocessor, defaultSource);
    SpanLogs logs = transformEvents(otlpSpan, span);
    if (!logs.getLogs().isEmpty()) {
      span.getAnnotations().add(new Annotation(SPAN_LOG_KEY, "true"));
    }

    return Pair.of(span, logs);
  }

  @VisibleForTesting
  public static Span transformSpan(io.opentelemetry.proto.trace.v1.Span otlpSpan,
                                   List<KeyValue> resourceAttrs,
                                   ReportableEntityPreprocessor preprocessor,
                                   String defaultSource) {
    Pair<String, List<KeyValue>> sourceAndResourceAttrs =
        sourceFromAttributes(resourceAttrs, defaultSource);
    String source = sourceAndResourceAttrs._1;
    resourceAttrs = sourceAndResourceAttrs._2;

    // Order of arguments to Stream.of() matters: when a Resource Attribute and a Span Attribute
    // happen to share the same key, we want the Span Attribute to "win" and be preserved.
    List<KeyValue> otlpAttributes = Stream.of(resourceAttrs, otlpSpan.getAttributesList())
        .flatMap(Collection::stream).collect(Collectors.toList());

    List<Annotation> wfAnnotations = annotationsFromAttributes(otlpAttributes);

    wfAnnotations.add(SPAN_KIND_ANNOTATION_HASH_MAP.get(otlpSpan.getKind()));
    wfAnnotations.addAll(annotationsFromStatus(otlpSpan.getStatus()));

    if (!otlpSpan.getParentSpanId().equals(ByteString.EMPTY)) {
      wfAnnotations.add(
          new Annotation(PARENT_KEY, SpanUtils.toStringId(otlpSpan.getParentSpanId()))
      );
    }

    String wfSpanId = SpanUtils.toStringId(otlpSpan.getSpanId());
    String wfTraceId = SpanUtils.toStringId(otlpSpan.getTraceId());
    long startTimeMs = TimeUnit.NANOSECONDS.toMillis(otlpSpan.getStartTimeUnixNano());
    long durationMs = otlpSpan.getEndTimeUnixNano() == 0 ? 0 :
        TimeUnit.NANOSECONDS.toMillis(otlpSpan.getEndTimeUnixNano() - otlpSpan.getStartTimeUnixNano());

    wavefront.report.Span toReturn = wavefront.report.Span.newBuilder()
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
  public static SpanLogs transformEvents(io.opentelemetry.proto.trace.v1.Span otlpSpan,
                                         Span wfSpan) {
    ArrayList<SpanLog> logs = new ArrayList<>();

    for (io.opentelemetry.proto.trace.v1.Span.Event event : otlpSpan.getEventsList()) {
      SpanLog log = new SpanLog();
      log.setTimestamp(TimeUnit.NANOSECONDS.toMicros(event.getTimeUnixNano()));
      Map<String, String> fields = mapFromAttributes(event.getAttributesList());
      fields.put(SPAN_EVENT_TAG_KEY, event.getName());
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
  public static Pair<String, List<KeyValue>> sourceFromAttributes(List<KeyValue> otlpAttributes,
                                                                  String defaultSource) {
    // Order of keys in List matters: it determines precedence when multiple candidates exist.
    List<String> candidateKeys = Arrays.asList(SOURCE_KEY, "host.name", "hostname", "host.id");
    Comparator<KeyValue> keySorter = Comparator.comparing(kv -> candidateKeys.indexOf(kv.getKey()));

    Optional<KeyValue> sourceAttr = otlpAttributes.stream()
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

  public static List<Annotation> annotationsFromAttributes(List<KeyValue> attributesList) {
    List<Annotation> annotations = Lists.newArrayList();
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

  public static Map<String, String> mapFromAttributes(List<KeyValue> attributes) {
    Map<String, String> map = new HashMap<>();
    for (KeyValue attribute : attributes) {
      map.put(attribute.getKey(), fromAnyValue(attribute.getValue()));
    }
    return map;
  }

  private static List<Annotation> annotationsFromStatus(Status otlpStatus) {
    if (otlpStatus.getCode() != Status.StatusCode.STATUS_CODE_ERROR) return Collections.emptyList();

    List<Annotation> statusAnnotations = Lists.newArrayList();
    statusAnnotations.add(new Annotation(ERROR_TAG_KEY, ERROR_SPAN_TAG_VAL));

    if (!otlpStatus.getMessage().isEmpty()) {
      statusAnnotations.add(new Annotation(OTEL_STATUS_DESCRIPTION_KEY, otlpStatus.getMessage()));
    }
    return statusAnnotations;
  }

  @VisibleForTesting
  static List<Annotation> setRequiredTags(List<Annotation> annotationList) {
    // TODO: remove conversion to Map and use streaming API
    Map<String, String> tags = Maps.newHashMap();
    for (Annotation annotation : annotationList) {
      tags.put(annotation.getKey(), annotation.getValue());
    }
    List<Annotation> requiredTags = Lists.newArrayList();

    if (!tags.containsKey(SERVICE_TAG_KEY)) {
      tags.put(SERVICE_TAG_KEY, tags.getOrDefault(SERVICE_NAME.getKey(), DEFAULT_SERVICE_NAME));
    }
    tags.remove(SERVICE_NAME.getKey());

    tags.putIfAbsent(APPLICATION_TAG_KEY, DEFAULT_APPLICATION_NAME);
    tags.putIfAbsent(CLUSTER_TAG_KEY, NULL_TAG_VAL);
    tags.putIfAbsent(SHARD_TAG_KEY, NULL_TAG_VAL);

    for (Map.Entry<String, String> tagEntry : tags.entrySet()) {
      requiredTags.add(
          Annotation.newBuilder()
              .setKey(tagEntry.getKey())
              .setValue(tagEntry.getValue())
              .build()
      );
    }

    return requiredTags;
  }

  /**
   * Converts an OTLP AnyValue object to its equivalent String representation.
   * The implementation mimics {@code AsString()} from the OpenTelemetry Collector:
   * https://github.com/open-telemetry/opentelemetry-collector/blob/cffbecb2ac9ee98e6a60d22f910760be48a94c55/model/pdata/common.go#L384
   *
   * <p>We do not handle {@code KvlistValue} because the OpenTelemetry Specification for
   * Attributes does not include maps as an allowed type of value:
   * https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/common/common.md#attributes
   *
   * @param anyValue OTLP Attributes value in {@link AnyValue} format
   * @return String representation of the {@link AnyValue}
   */
  private static String fromAnyValue(AnyValue anyValue) {
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
      return values.stream().map(OtlpProtobufUtils::fromAnyValue)
          .collect(Collectors.joining(", ", "[", "]"));
    } else if (anyValue.hasKvlistValue()) {
        OTLP_DATA_LOGGER.finest(() -> "Encountered KvlistValue but cannot convert to String");
    } else if (anyValue.hasBytesValue()) {
      return Base64.getEncoder().encodeToString(anyValue.getBytesValue().toByteArray());
    }
    return "<Unknown OpenTelemetry attribute value type " + anyValue.getValueCase() + ">";
  }
}
