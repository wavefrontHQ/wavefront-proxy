package com.wavefront.agent.listeners.otlp;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.wavefront.agent.listeners.tracing.SpanUtils;
import com.wavefront.sdk.common.Constants;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import wavefront.report.Annotation;
import wavefront.report.Span;

import static com.wavefront.common.TraceConstants.PARENT_KEY;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static io.opentelemetry.semconv.resource.attributes.ResourceAttributes.SERVICE_NAME;

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
public class OtlpProtobufUtils {
  private final static String DEFAULT_APPLICATION_NAME = "defaultApplication";
  private final static String DEFAULT_SERVICE_NAME = "defaultService";
  private final static String DEFAULT_SOURCE = "otlp";

  /**
   * OpenTelemetry Span/Trace ID length specification ref: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#spancontext
   * <p>
   * TraceId A valid trace identifier is a 16-byte array with at least one non-zero byte.
   * SpanId A valid span identifier is an 8-byte array with at least one non-zero byte.
   */
  private final static Logger logger = Logger.getLogger(OtlpProtobufUtils.class.getCanonicalName());

  // TODO: consider transforming a single span and returning it for immedidate reporting in
  //   wfSender. This could be more efficient, and also more reliable in the event the loops
  //   below throw an error and we don't report any of the list.
  public static List<Span> otlpSpanExportRequestParseToWFSpan(ExportTraceServiceRequest request) {
    List<Span> wfSpans = Lists.newArrayList();
    for (ResourceSpans resourceSpans : request.getResourceSpansList()) {
      for (InstrumentationLibrarySpans instrumentationLibrarySpans :
          resourceSpans.getInstrumentationLibrarySpansList()) {
        for (io.opentelemetry.proto.trace.v1.Span otlpSpan : instrumentationLibrarySpans.getSpansList()) {
          wavefront.report.Span wfSpan = transform(otlpSpan, resourceSpans.getResource().getAttributesList());
          logger.info("Transformed OTLP into WF span: " + wfSpan);
          wfSpans.add(wfSpan);
        }
      }
    }
    return wfSpans;
  }

  public static wavefront.report.Span transform(io.opentelemetry.proto.trace.v1.Span otlpSpan,
                                                List<KeyValue> resourceAttrs) {
    String wfSpanId = SpanUtils.toStringId(otlpSpan.getSpanId());
    String wfTraceId = SpanUtils.toStringId(otlpSpan.getTraceId());
    long startTimeMs = otlpSpan.getStartTimeUnixNano() / 1000;
    long durationMs = otlpSpan.getEndTimeUnixNano() == 0 ? 0 :
        (otlpSpan.getEndTimeUnixNano() - otlpSpan.getStartTimeUnixNano()) / 1000;

    List<KeyValue> attributesList = Lists.newArrayList();
    if (resourceAttrs != null) {
      attributesList.addAll(resourceAttrs);
    }
    attributesList.addAll(otlpSpan.getAttributesList());
    // convert attributions to WF annotations and try to patch WF required annotations
    List<Annotation> annotationList = attributesToWFAnnotations(attributesList);
    if (!otlpSpan.getParentSpanId().equals(ByteString.EMPTY)) {
      annotationList.add(
          Annotation.newBuilder()
              .setKey(PARENT_KEY)
              .setValue(SpanUtils.toStringId(otlpSpan.getParentSpanId()))
              .build());
    }

    wavefront.report.Span toReturn = wavefront.report.Span.newBuilder()
        .setName(otlpSpan.getName())
        .setSpanId(wfSpanId)
        .setTraceId(wfTraceId)
        .setStartMillis(startTimeMs)
        .setDuration(durationMs)
        .setAnnotations(annotationList)
        // TODO: Check the precedence about the source tag
        .setSource(DEFAULT_SOURCE)
        .setCustomer("dummy")
        .build();
    return toReturn;
  }

  public static List<Annotation> attributesToWFAnnotations(List<KeyValue> attributesList) {
    Map<String, String> tags = Maps.newHashMap();
    for (KeyValue attribute : attributesList) {
      if (!attribute.hasValue()) {
        tags.put(attribute.getKey(), "");
      } else {
        tags.put(attribute.getKey(), fromAnyValue(attribute.getValue()));
      }
    }
    return setRequiredTags(tags);
  }

  private static List<Annotation> setRequiredTags(Map<String, String> tags) {
    List<Annotation> requiredTags = Lists.newArrayList();

    if (!tags.containsKey(SERVICE_TAG_KEY)) {
      if (tags.containsKey(SERVICE_NAME.getKey())) {
        tags.put(SERVICE_TAG_KEY, tags.get(SERVICE_NAME.getKey()));
        tags.remove(SERVICE_NAME.getKey());
      } else {
        tags.put(SERVICE_TAG_KEY, DEFAULT_SERVICE_NAME);
      }
    }

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
      // TODO
      // Golang implementation: https://github.com/open-telemetry/opentelemetry-collector/blob/7ed3f75ef84d9e9d11b175a0859060f765faca0b/model/pdata/common.go#L381-L384
      // ref: https://github.com/open-telemetry/opentelemetry-proto/blob/27a10cd70f63afdbddf460881969f9ad7ae4af5d/opentelemetry/proto/common/v1/common.proto
    } else if (anyValue.hasKvlistValue()) {
      // TODO: see above for implementation
    } else if (anyValue.hasBytesValue()) {
      // TODO: see above for implementation
    }
    return "";
  }
}
