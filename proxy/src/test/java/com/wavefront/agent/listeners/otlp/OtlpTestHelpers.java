package com.wavefront.agent.listeners.otlp;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.wavefront.common.Pair;

import org.apache.commons.compress.utils.Lists;

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
  private static final long startTimeMs = System.currentTimeMillis();
  private static final long durationMs = 50L;
  private static final byte[] spanIdBytes = {0x9, 0x9, 0x9, 0x9, 0x9, 0x9, 0x9, 0x9};
  private static final byte[] parentSpanIdBytes = {0x6, 0x6, 0x6, 0x6, 0x6, 0x6, 0x6, 0x6};
  private static final byte[] traceIdBytes = {0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1,
      0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1};

  public static Span.Builder wfSpanGenerator(@Nullable List<Annotation> extraAttrs) {
    List<Annotation> annotations = Lists.newArrayList();
    annotations.add(
        Annotation.newBuilder()
            .setKey(APPLICATION_TAG_KEY)
            .setValue("defaultApplication")
            .build()
    );
    annotations.add(
        Annotation.newBuilder()
            .setKey(SERVICE_TAG_KEY)
            .setValue("defaultService")
            .build()
    );
    annotations.add(
        Annotation.newBuilder()
            .setKey("cluster")
            .setValue("none")
            .build()
    );
    annotations.add(
        Annotation.newBuilder()
            .setKey("shard")
            .setValue("none")
            .build()
    );

    if (extraAttrs != null) {
      annotations.addAll(extraAttrs);
    }

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
        .setSource("otlp")
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

  public static Pair<ByteString, String> parentSpanIdPair() {
    return Pair.of(ByteString.copyFrom(parentSpanIdBytes), "00000000-0000-0000-0606-060606060606");
  }

  public static void assertWFSpanEquals(wavefront.report.Span s1, wavefront.report.Span s2) {
    assertEquals(s1.getName(), s2.getName());
    assertEquals(s1.getSpanId(), s2.getSpanId());
    assertEquals(s1.getTraceId(), s2.getTraceId());
    assertEquals(s1.getStartMillis(), s2.getStartMillis());
    assertEquals(s1.getDuration(), s2.getDuration());
    assertEquals(s1.getSource(), s2.getSource());
    assertEquals(s1.getCustomer(), s2.getCustomer());

    Map<String, String> s1AnnotationMap = annotationListToMap(s1.getAnnotations());
    assertEquals(s1.getAnnotations().size(), s2.getAnnotations().size());
    for (Annotation s2Annotation : s2.getAnnotations()) {
      assertTrue(s1AnnotationMap.containsKey(s2Annotation.getKey()));
      assertEquals(s1AnnotationMap.get(s2Annotation.getKey()), s2Annotation.getValue());
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
