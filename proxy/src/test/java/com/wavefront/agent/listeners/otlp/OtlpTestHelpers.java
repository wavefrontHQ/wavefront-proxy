package com.wavefront.agent.listeners.otlp;

import com.google.protobuf.ByteString;

import com.wavefront.agent.preprocessor.PreprocessorRuleMetrics;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.preprocessor.SpanAddAnnotationIfNotExistsTransformer;
import com.wavefront.agent.preprocessor.SpanBlockFilter;
import com.wavefront.sdk.common.Pair;

import org.apache.commons.compress.utils.Lists;
import org.hamcrest.FeatureMatcher;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Status;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;

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

  public static FeatureMatcher<List<Annotation>, Iterable<String>> hasKey(String key) {
    return new FeatureMatcher<List<Annotation>, Iterable<String>>(hasItem(key),
        "Annotations with Keys", "Annotation Key") {
      @Override
      protected Iterable<String> featureValueOf(List<Annotation> actual) {
        return actual.stream().map(Annotation::getKey).collect(Collectors.toList());
      }
    };
  }

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

    return wavefront.report.Span.newBuilder()
        .setName("root")
        .setSpanId("00000000-0000-0000-0909-090909090909")
        .setTraceId("01010101-0101-0101-0101-010101010101")
        .setStartMillis(startTimeMs)
        .setDuration(durationMs)
        .setAnnotations(annotations)
        .setSource("test-source")
        .setCustomer("dummy");
  }

  public static SpanLogs.Builder wfSpanLogsGenerator(Span span) {
    long logTimestamp =
        TimeUnit.MILLISECONDS.toMicros(span.getStartMillis() + (span.getDuration() / 2));
    Map<String, String> logFields = new HashMap<String, String>() {{
      put("name", "eventName");
      put("attrKey", "attrValue");
    }};
    SpanLog spanLog = SpanLog.newBuilder().setFields(logFields).setTimestamp(logTimestamp).build();

    return SpanLogs.newBuilder()
        .setLogs(Collections.singletonList(spanLog))
        .setSpanId(span.getSpanId())
        .setTraceId(span.getTraceId())
        .setCustomer(span.getCustomer());
  }


  public static io.opentelemetry.proto.trace.v1.Span.Builder otlpSpanGenerator() {
    return io.opentelemetry.proto.trace.v1.Span.newBuilder()
        .setName("root")
        .setSpanId(ByteString.copyFrom(spanIdBytes))
        .setTraceId(ByteString.copyFrom(traceIdBytes))
        .setStartTimeUnixNano(TimeUnit.MILLISECONDS.toNanos(startTimeMs))
        .setEndTimeUnixNano(TimeUnit.MILLISECONDS.toNanos(startTimeMs + durationMs));
  }

  public static io.opentelemetry.proto.trace.v1.Span otlpSpanWithKind(
      io.opentelemetry.proto.trace.v1.Span.SpanKind kind) {
    return otlpSpanGenerator().setKind(kind).build();
  }

  public static io.opentelemetry.proto.trace.v1.Span otlpSpanWithStatus(Status.StatusCode code,
                                                                        String message) {
    Status status = Status.newBuilder().setCode(code).setMessage(message).build();
    return otlpSpanGenerator().setStatus(status).build();
  }

  public static io.opentelemetry.proto.common.v1.KeyValue otlpAttribute(String key, String value) {
    return KeyValue.newBuilder().setKey(key).setValue(
        AnyValue.newBuilder().setStringValue(value).build()
    ).build();
  }

  public static io.opentelemetry.proto.trace.v1.Span.Event otlpSpanEvent() {
    long eventTimestamp = TimeUnit.MILLISECONDS.toNanos(startTimeMs + (durationMs / 2));
    KeyValue attr = otlpAttribute("attrKey", "attrValue");
    return io.opentelemetry.proto.trace.v1.Span.Event.newBuilder()
        .setName("eventName")
        .setTimeUnixNano(eventTimestamp)
        .addAttributes(attr)
        .build();
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
        "sourceName", "test-source", x -> true, preprocessorRuleMetrics));

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

    assertThat("Annotations match in any order", actual.getAnnotations(),
        containsInAnyOrder(expected.getAnnotations().toArray()));
  }

  public static ExportTraceServiceRequest otlpTraceRequest(io.opentelemetry.proto.trace.v1.Span otlpSpan) {
    InstrumentationLibrarySpans ilSpans = InstrumentationLibrarySpans.newBuilder().addSpans(otlpSpan).build();
    ResourceSpans rSpans = ResourceSpans.newBuilder().addInstrumentationLibrarySpans(ilSpans).build();
    ExportTraceServiceRequest request = ExportTraceServiceRequest.newBuilder().addResourceSpans(rSpans).build();
    return request;
  }

}
