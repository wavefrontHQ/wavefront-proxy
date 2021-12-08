package com.wavefront.agent.listeners.otlp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.listeners.tracing.SpanUtils;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;

import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Level;
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

import static com.wavefront.common.TraceConstants.PARENT_KEY;
import static com.wavefront.internal.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.ERROR_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
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
  private final static String DEFAULT_SOURCE = "otlp";
  private final static Logger OTLP_DATA_LOGGER = Logger.getLogger("OTLPDataLogger");
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
                                       @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier
  ) {
    ReportableEntityPreprocessor preprocessor = null;
    if (preprocessorSupplier != null) {
      preprocessor = preprocessorSupplier.get();
    }

    for (wavefront.report.Span wfSpan :
        OtlpProtobufUtils.otlpSpanExportRequestParseToWFSpan(request, preprocessor)) {
      // TODO: handle sampler
      if (!wasFilteredByPreprocessor(wfSpan, spanHandler, preprocessor)) {
        spanHandler.report(wfSpan);
      }
    }
  }

  // TODO: consider transforming a single span and returning it for immedidate reporting in
  //   wfSender. This could be more efficient, and also more reliable in the event the loops
  //   below throw an error and we don't report any of the list.
  private static List<Span> otlpSpanExportRequestParseToWFSpan(ExportTraceServiceRequest request,
                                                               @Nullable ReportableEntityPreprocessor preprocessor) {
    List<Span> wfSpans = Lists.newArrayList();
    for (ResourceSpans resourceSpans : request.getResourceSpansList()) {
      Resource resource = resourceSpans.getResource();
      if (OTLP_DATA_LOGGER.isLoggable(Level.FINEST)) {
        OTLP_DATA_LOGGER.info("Inbound OTLP Resource: " + resource);
      }
      for (InstrumentationLibrarySpans instrumentationLibrarySpans :
          resourceSpans.getInstrumentationLibrarySpansList()) {
        if (OTLP_DATA_LOGGER.isLoggable(Level.FINEST)) {
          OTLP_DATA_LOGGER.info("Inbound OTLP Instrumentation Library: " +
              instrumentationLibrarySpans.getInstrumentationLibrary());
        }
        for (io.opentelemetry.proto.trace.v1.Span otlpSpan : instrumentationLibrarySpans.getSpansList()) {
          wavefront.report.Span wfSpan = transform(otlpSpan, resource.getAttributesList(), preprocessor);
          if (OTLP_DATA_LOGGER.isLoggable(Level.FINEST)) {
            OTLP_DATA_LOGGER.info("Inbound OTLP Span: " + otlpSpan);
            OTLP_DATA_LOGGER.info("Converted Wavefront Span: " + wfSpan);
          }

          wfSpans.add(wfSpan);
        }
      }
    }
    return wfSpans;
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

  public static wavefront.report.Span transform(io.opentelemetry.proto.trace.v1.Span otlpSpan,
                                                List<KeyValue> resourceAttrs,
                                                ReportableEntityPreprocessor preprocessor) {
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
    long startTimeMs = otlpSpan.getStartTimeUnixNano() / 1000;
    long durationMs = otlpSpan.getEndTimeUnixNano() == 0 ? 0 :
        (otlpSpan.getEndTimeUnixNano() - otlpSpan.getStartTimeUnixNano()) / 1000;

    wavefront.report.Span toReturn = wavefront.report.Span.newBuilder()
        .setName(otlpSpan.getName())
        .setSpanId(wfSpanId)
        .setTraceId(wfTraceId)
        .setStartMillis(startTimeMs)
        .setDuration(durationMs)
        .setAnnotations(wfAnnotations)
        // TODO: Check the precedence about the source tag
        .setSource(DEFAULT_SOURCE)
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

  public static List<Annotation> annotationsFromAttributes(List<KeyValue> attributesList) {
    List<Annotation> annotations = Lists.newArrayList();
    for (KeyValue attribute : attributesList) {
      Annotation.Builder annotationBuilder = Annotation.newBuilder().setKey(attribute.getKey());
      if (!attribute.hasValue()) {
        annotationBuilder.setValue("");
      } else {
        annotationBuilder.setValue(fromAnyValue(attribute.getValue()));
      }
      annotations.add(annotationBuilder.build());
    }
    return annotations;
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
      StringBuilder sb = new StringBuilder();
      sb.append('[');
      for (AnyValue value : values) {
        // recursive call fromAnyValue()
        sb.append(fromAnyValue(value));
        sb.append(", ");
      }
      // remove the last ", " in the end
      sb.deleteCharAt(sb.length() - 1);
      sb.deleteCharAt(sb.length() - 1);
      sb.append(']');
      return sb.toString();
    } else if (anyValue.hasKvlistValue()) {
      if (OTLP_DATA_LOGGER.isLoggable(Level.FINEST)) {
        OTLP_DATA_LOGGER.severe("Encountered KvlistValue but cannot convert to String");
      }
    } else if (anyValue.hasBytesValue()) {
      return Base64.getEncoder().encodeToString(anyValue.getBytesValue().toByteArray());
    }
    return "<Unknown OpenTelemetry attribute value type " + anyValue.getValueCase() + ">";
  }
}
