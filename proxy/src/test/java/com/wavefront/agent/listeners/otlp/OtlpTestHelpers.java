package com.wavefront.agent.listeners.otlp;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.wavefront.agent.preprocessor.PreprocessorRuleMetrics;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.preprocessor.SpanAddAnnotationIfNotExistsTransformer;
import com.wavefront.agent.preprocessor.SpanBlockFilter;
import com.wavefront.common.Pair;

import org.apache.commons.compress.utils.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import wavefront.report.Annotation;
import wavefront.report.Span;

import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
public class OtlpTestHelpers {
  public static final String DEFAULT_SOURCE = "otlp";
  private static final long startTimeMs = System.currentTimeMillis();
  private static final long durationMs = 50L;
  private static final byte[] spanIdBytes = {0x9, 0x9, 0x9, 0x9, 0x9, 0x9, 0x9, 0x9};
  private static final byte[] parentSpanIdBytes = {0x6, 0x6, 0x6, 0x6, 0x6, 0x6, 0x6, 0x6};
  private static final byte[] traceIdBytes = {0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1,
      0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1};

  public static Span.Builder wfSpanGenerator(@Nullable List<Annotation> extraAttrs) {
    if (extraAttrs == null) {
      extraAttrs = Collections.emptyList();
    }
    List<Annotation> annotations = Lists.newArrayList();
    if (extraAttrs.stream().noneMatch(anno -> anno.getKey().equals(APPLICATION_TAG_KEY))) {
      annotations.add(new Annotation(APPLICATION_TAG_KEY, "defaultApplication"));
    }
    if (extraAttrs.stream().noneMatch(anno -> anno.getKey().equals(SERVICE_TAG_KEY))) {
      annotations.add(new Annotation(SERVICE_TAG_KEY, "defaultService"));
    }
    if (extraAttrs.stream().noneMatch(anno -> anno.getKey().equals("cluster"))) {
      annotations.add(new Annotation("cluster", "none"));
    }
    if (extraAttrs.stream().noneMatch(anno -> anno.getKey().equals("shard"))) {
      annotations.add(new Annotation("shard", "none"));
    }
    if (extraAttrs.stream().noneMatch(anno -> anno.getKey().equals("span.kind"))) {
      annotations.add(new Annotation("span.kind", "unspecified"));
    }

    annotations.addAll(extraAttrs);

    // reorder the annotations
    Map<String, String> map = Maps.newHashMap();
    for (Annotation annotation : annotations) {
      map.put(annotation.getKey(), annotation.getValue());
    }
    assertEquals(annotations.size(), map.size());
    annotations.clear();
    for (Map.Entry<String, String> mapEntry : map.entrySet()) {
      annotations.add(Annotation.newBuilder().setKey(mapEntry.getKey()).setValue(mapEntry.getValue()).build());
    }

    return wavefront.report.Span.newBuilder()
        .setName("root")
        .setSpanId("00000000-0000-0000-0909-090909090909")
        .setTraceId("01010101-0101-0101-0101-010101010101")
        .setStartMillis(startTimeMs)
        .setDuration(durationMs)
        .setAnnotations(annotations)
        .setSource(DEFAULT_SOURCE)
        .setCustomer("dummy");
  }

  public static io.opentelemetry.proto.trace.v1.Span.Builder otlpSpanGenerator() {
    return io.opentelemetry.proto.trace.v1.Span.newBuilder()
        .setName("root")
        .setSpanId(ByteString.copyFrom(spanIdBytes))
        .setTraceId(ByteString.copyFrom(traceIdBytes))
        .setStartTimeUnixNano(startTimeMs * 1000)
        .setEndTimeUnixNano((startTimeMs + durationMs) * 1000);
  }

  public static io.opentelemetry.proto.trace.v1.Span otlpSpanWithKind(
      io.opentelemetry.proto.trace.v1.Span.SpanKind kind) {
    return otlpSpanGenerator().setKind(kind).build();
  }

  public static Pair<ByteString, String> parentSpanIdPair() {
    return Pair.of(ByteString.copyFrom(parentSpanIdBytes), "00000000-0000-0000-0606-060606060606");
  }

  public static ReportableEntityPreprocessor addTagIfNotExistsPreprocessor(List<Annotation> annotationList) {
    ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
    PreprocessorRuleMetrics preprocessorRuleMetrics = new PreprocessorRuleMetrics(null, null,
        null);
    for (Annotation annotation : annotationList) {
      preprocessor.forSpan().addTransformer(new SpanAddAnnotationIfNotExistsTransformer(
          annotation.getKey(), annotation.getValue(), x -> true, preprocessorRuleMetrics));
    }

    return preprocessor;
  }

  public static ReportableEntityPreprocessor blockSpanPreprocessor() {
    ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
    PreprocessorRuleMetrics preprocessorRuleMetrics = new PreprocessorRuleMetrics(null, null,
        null);
    preprocessor.forSpan().addFilter(new SpanBlockFilter(
        "sourceName", DEFAULT_SOURCE, x -> true, preprocessorRuleMetrics));

    return preprocessor;
  }

  public static ReportableEntityPreprocessor rejectSpanPreprocessor() {
    ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
    preprocessor.forSpan().addFilter((input, messageHolder) -> {
      if (messageHolder != null && messageHolder.length > 0) {
        messageHolder[0] = "span rejected for testing purpose";
      }
      return false;
    });

    return preprocessor;
  }

  public static void assertWFSpanEquals(wavefront.report.Span expected, wavefront.report.Span actual) {
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getSpanId(), actual.getSpanId());
    assertEquals(expected.getTraceId(), actual.getTraceId());
    assertEquals(expected.getStartMillis(), actual.getStartMillis());
    assertEquals(expected.getDuration(), actual.getDuration());
    assertEquals(expected.getSource(), actual.getSource());
    assertEquals(expected.getCustomer(), actual.getCustomer());

    Map<String, String> expectedAnnotationMap = annotationListToMap(expected.getAnnotations());
    assertEquals(expected.getAnnotations().size(), actual.getAnnotations().size());
    for (Annotation actualAnnotation : actual.getAnnotations()) {
      assertTrue(expectedAnnotationMap.containsKey(actualAnnotation.getKey()));
      assertEquals(expectedAnnotationMap.get(actualAnnotation.getKey()), actualAnnotation.getValue());
    }
  }

  private static Map<String, String> annotationListToMap(List<Annotation> annotationList) {
    Map<String, String> annotationMap = Maps.newHashMap();
    for (Annotation annotation : annotationList) {
      annotationMap.put(annotation.getKey(), annotation.getValue());
    }
    assertEquals(annotationList.size(), annotationMap.size());
    return annotationMap;
  }
}
