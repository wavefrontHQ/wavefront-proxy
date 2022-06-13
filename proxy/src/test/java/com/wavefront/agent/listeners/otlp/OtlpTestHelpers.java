package com.wavefront.agent.listeners.otlp;

import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.wavefront.agent.preprocessor.PreprocessorRuleMetrics;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.preprocessor.SpanAddAnnotationIfNotExistsTransformer;
import com.wavefront.agent.preprocessor.SpanBlockFilter;
import com.wavefront.sdk.common.Pair;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Status;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.compress.utils.Lists;
import org.hamcrest.FeatureMatcher;
import wavefront.report.Annotation;
import wavefront.report.Histogram;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
public class OtlpTestHelpers {
  public static final String DEFAULT_SOURCE = "test-source";
  private static final long startTimeMs = System.currentTimeMillis();
  private static final long durationMs = 50L;
  private static final byte[] spanIdBytes = {0x9, 0x9, 0x9, 0x9, 0x9, 0x9, 0x9, 0x9};
  private static final byte[] parentSpanIdBytes = {0x6, 0x6, 0x6, 0x6, 0x6, 0x6, 0x6, 0x6};
  private static final byte[] traceIdBytes = {
    0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1
  };

  public static FeatureMatcher<List<Annotation>, Iterable<String>> hasKey(String key) {
    return new FeatureMatcher<List<Annotation>, Iterable<String>>(
        hasItem(key), "Annotations with Keys", "Annotation Key") {
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
        .setSource(DEFAULT_SOURCE)
        .setCustomer("dummy");
  }

  public static SpanLogs.Builder wfSpanLogsGenerator(Span span, int droppedAttrsCount) {
    long logTimestamp =
        TimeUnit.MILLISECONDS.toMicros(span.getStartMillis() + (span.getDuration() / 2));
    Map<String, String> logFields =
        new HashMap<String, String>() {
          {
            put("name", "eventName");
            put("attrKey", "attrValue");
          }
        };

    // otel spec says it's invalid to add the tag if the count is zero
    if (droppedAttrsCount > 0) {
      logFields.put("otel.dropped_attributes_count", String.valueOf(droppedAttrsCount));
    }
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

  public static io.opentelemetry.proto.trace.v1.Span otlpSpanWithStatus(
      Status.StatusCode code, String message) {
    Status status = Status.newBuilder().setCode(code).setMessage(message).build();
    return otlpSpanGenerator().setStatus(status).build();
  }

  public static io.opentelemetry.proto.common.v1.KeyValue attribute(String key, String value) {
    return KeyValue.newBuilder()
        .setKey(key)
        .setValue(AnyValue.newBuilder().setStringValue(value).build())
        .build();
  }

  public static io.opentelemetry.proto.trace.v1.Span.Event otlpSpanEvent(int droppedAttrsCount) {
    long eventTimestamp = TimeUnit.MILLISECONDS.toNanos(startTimeMs + (durationMs / 2));
    KeyValue attr = attribute("attrKey", "attrValue");
    io.opentelemetry.proto.trace.v1.Span.Event.Builder builder =
        io.opentelemetry.proto.trace.v1.Span.Event.newBuilder()
            .setName("eventName")
            .setTimeUnixNano(eventTimestamp)
            .addAttributes(attr);

    if (droppedAttrsCount > 0) {
      builder.setDroppedAttributesCount(droppedAttrsCount);
    }
    return builder.build();
  }

  public static Pair<ByteString, String> parentSpanIdPair() {
    return Pair.of(ByteString.copyFrom(parentSpanIdBytes), "00000000-0000-0000-0606-060606060606");
  }

  public static ReportableEntityPreprocessor addTagIfNotExistsPreprocessor(
      List<Annotation> annotationList) {
    ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
    PreprocessorRuleMetrics preprocessorRuleMetrics = new PreprocessorRuleMetrics(null, null, null);
    for (Annotation annotation : annotationList) {
      preprocessor
          .forSpan()
          .addTransformer(
              new SpanAddAnnotationIfNotExistsTransformer(
                  annotation.getKey(), annotation.getValue(), x -> true, preprocessorRuleMetrics));
    }

    return preprocessor;
  }

  public static ReportableEntityPreprocessor blockSpanPreprocessor() {
    ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
    PreprocessorRuleMetrics preprocessorRuleMetrics = new PreprocessorRuleMetrics(null, null, null);
    preprocessor
        .forSpan()
        .addFilter(
            new SpanBlockFilter("sourceName", DEFAULT_SOURCE, x -> true, preprocessorRuleMetrics));

    return preprocessor;
  }

  public static ReportableEntityPreprocessor rejectSpanPreprocessor() {
    ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
    preprocessor
        .forSpan()
        .addFilter(
            (input, messageHolder) -> {
              if (messageHolder != null && messageHolder.length > 0) {
                messageHolder[0] = "span rejected for testing purpose";
              }
              return false;
            });

    return preprocessor;
  }

  public static void assertWFSpanEquals(
      wavefront.report.Span expected, wavefront.report.Span actual) {
    assertEquals(expected.getName(), actual.getName());
    assertEquals(expected.getSpanId(), actual.getSpanId());
    assertEquals(expected.getTraceId(), actual.getTraceId());
    assertEquals(expected.getStartMillis(), actual.getStartMillis());
    assertEquals(expected.getDuration(), actual.getDuration());
    assertEquals(expected.getSource(), actual.getSource());
    assertEquals(expected.getCustomer(), actual.getCustomer());

    assertThat(
        "Annotations match in any order",
        actual.getAnnotations(),
        containsInAnyOrder(expected.getAnnotations().toArray()));
  }

  public static ExportTraceServiceRequest otlpTraceRequest(
      io.opentelemetry.proto.trace.v1.Span otlpSpan) {
    ScopeSpans scopeSpans = ScopeSpans.newBuilder().addSpans(otlpSpan).build();
    ResourceSpans rSpans = ResourceSpans.newBuilder().addScopeSpans(scopeSpans).build();
    return ExportTraceServiceRequest.newBuilder().addResourceSpans(rSpans).build();
  }

  private static void assertHistogramsEqual(Histogram expected, Histogram actual, double delta) {
    String errorSuffix = " mismatched. Expected: " + expected + " ,Actual: " + actual;
    assertEquals("Histogram duration" + errorSuffix, expected.getDuration(), actual.getDuration());
    assertEquals("Histogram type" + errorSuffix, expected.getType(), actual.getType());
    List<Double> expectedBins = expected.getBins();
    List<Double> actualBins = actual.getBins();
    assertEquals("Histogram bin size" + errorSuffix, expectedBins.size(), actualBins.size());
    for (int i = 0; i < expectedBins.size(); i++) {
      assertEquals(
          "Histogram bin " + i + errorSuffix, expectedBins.get(i), actualBins.get(i), delta);
    }
    assertEquals("Histogram counts" + errorSuffix, expected.getCounts(), actual.getCounts());
  }

  public static void assertWFReportPointEquals(
      wavefront.report.ReportPoint expected, wavefront.report.ReportPoint actual) {
    assertEquals("metric name", expected.getMetric(), actual.getMetric());
    Object expectedValue = expected.getValue();
    Object actualValue = actual.getValue();
    if ((expectedValue instanceof Histogram) && (actualValue instanceof Histogram)) {
      assertHistogramsEqual((Histogram) expectedValue, (Histogram) actualValue, 0.0001);
    } else {
      assertEquals("value", expectedValue, actualValue);
    }
    assertEquals("timestamp", expected.getTimestamp(), actual.getTimestamp());
    assertEquals(
        "number of annotations", expected.getAnnotations().size(), actual.getAnnotations().size());
    assertEquals("source/host", expected.getHost(), actual.getHost());
    // TODO use a better assert instead of iterating manually?
    for (String key : expected.getAnnotations().keySet()) {
      assertTrue(actual.getAnnotations().containsKey(key));
      assertEquals(expected.getAnnotations().get(key), actual.getAnnotations().get(key));
    }
  }

  public static void assertAllPointsEqual(
      List<wavefront.report.ReportPoint> expected, List<wavefront.report.ReportPoint> actual) {
    assertEquals("same number of points", expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertWFReportPointEquals(expected.get(i), actual.get(i));
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

  public static io.opentelemetry.proto.metrics.v1.Metric.Builder otlpMetricGenerator() {
    return io.opentelemetry.proto.metrics.v1.Metric.newBuilder().setName("test");
  }

  public static io.opentelemetry.proto.metrics.v1.Metric.Builder otlpGaugeGenerator(
      List<NumberDataPoint> points) {
    return otlpMetricGenerator()
        .setGauge(
            io.opentelemetry.proto.metrics.v1.Gauge.newBuilder().addAllDataPoints(points).build());
  }

  public static io.opentelemetry.proto.metrics.v1.Metric.Builder otlpGaugeGenerator(
      NumberDataPoint point) {
    return otlpMetricGenerator()
        .setGauge(
            io.opentelemetry.proto.metrics.v1.Gauge.newBuilder().addDataPoints(point).build());
  }

  public static io.opentelemetry.proto.metrics.v1.Metric.Builder otlpSumGenerator(
      List<NumberDataPoint> points) {
    return otlpMetricGenerator()
        .setSum(
            io.opentelemetry.proto.metrics.v1.Sum.newBuilder()
                .setAggregationTemporality(
                    io.opentelemetry.proto.metrics.v1.AggregationTemporality
                        .AGGREGATION_TEMPORALITY_CUMULATIVE)
                .setIsMonotonic(true)
                .addAllDataPoints(points)
                .build());
  }

  public static io.opentelemetry.proto.metrics.v1.Metric.Builder otlpSummaryGenerator(
      SummaryDataPoint point) {
    return otlpMetricGenerator()
        .setSummary(
            io.opentelemetry.proto.metrics.v1.Summary.newBuilder().addDataPoints(point).build());
  }

  public static io.opentelemetry.proto.metrics.v1.Metric.Builder otlpSummaryGenerator(
      Collection<SummaryDataPoint.ValueAtQuantile> quantiles) {
    return otlpSummaryGenerator(
        SummaryDataPoint.newBuilder().addAllQuantileValues(quantiles).build());
  }

  public static wavefront.report.ReportPoint.Builder wfReportPointGenerator() {
    return wavefront.report.ReportPoint.newBuilder()
        .setMetric("test")
        .setHost(DEFAULT_SOURCE)
        .setTimestamp(0)
        .setValue(0.0);
  }

  public static wavefront.report.ReportPoint.Builder wfReportPointGenerator(
      List<Annotation> annotations) {
    return wfReportPointGenerator().setAnnotations(annotationListToMap(annotations));
  }

  public static List<wavefront.report.ReportPoint> justThePointsNamed(
      String name, Collection<wavefront.report.ReportPoint> points) {
    return points.stream().filter(p -> p.getMetric().equals(name)).collect(Collectors.toList());
  }
}
