package com.wavefront.agent.listeners.otlp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.listeners.tracing.SpanUtils;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

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

  public static void exportToWavefront(ExportTraceServiceRequest request,
                         ReportableEntityHandler<Span, String> spanHandler,
                         @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier
                         ) {
    ReportableEntityPreprocessor preprocessor = null;
    if (preprocessorSupplier != null) {
      preprocessor = preprocessorSupplier.get();
    }

    for (wavefront.report.Span wfSpan:
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
      for (InstrumentationLibrarySpans instrumentationLibrarySpans :
          resourceSpans.getInstrumentationLibrarySpansList()) {
        for (io.opentelemetry.proto.trace.v1.Span otlpSpan : instrumentationLibrarySpans.getSpansList()) {
          wavefront.report.Span wfSpan = transform(otlpSpan, resourceSpans.getResource().getAttributesList(), preprocessor);
          logger.info("Transformed OTLP into WF span: " + wfSpan);

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
                                                List<KeyValue> resourceAttrs, ReportableEntityPreprocessor preprocessor) {
    String wfSpanId = SpanUtils.toStringId(otlpSpan.getSpanId());
    String wfTraceId = SpanUtils.toStringId(otlpSpan.getTraceId());
    long startTimeMs = otlpSpan.getStartTimeUnixNano() / 1000;
    long durationMs = otlpSpan.getEndTimeUnixNano() == 0 ? 0 :
        (otlpSpan.getEndTimeUnixNano() - otlpSpan.getStartTimeUnixNano()) / 1000;

    List<KeyValue> attributesList = Lists.newArrayList();
    if (resourceAttrs != null) {
      attributesList.addAll(resourceAttrs);
    }
    // TODO: otlpSpan.getAttributesList() should precedes resourceAttrs if has same key
    attributesList.addAll(otlpSpan.getAttributesList());
    // convert OTLP Attributes to WF annotations
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
    // apply preprocessor
    if (preprocessor != null) {
      preprocessor.forSpan().transform(toReturn);
    }

    // set required WF tags that may be missing
    List<Annotation> processedAnnotationList = setRequiredTags(toReturn.getAnnotations());
    toReturn.setAnnotations(processedAnnotationList);
    return toReturn;
  }

  public static List<Annotation> attributesToWFAnnotations(List<KeyValue> attributesList) {
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
      logger.log(Level.SEVERE, "Encountered ArrayValue but cannot convert to String");
    } else if (anyValue.hasKvlistValue()) {
      // TODO: see above for implementation
      logger.log(Level.SEVERE, "Encountered KvlistValue but cannot convert to String");
    } else if (anyValue.hasBytesValue()) {
      // TODO: see above for implementation
      logger.log(Level.SEVERE, "Encountered BytesValue but cannot convert to String");
    }
    return "";
  }
}
