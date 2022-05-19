package com.wavefront.agent.listeners.otlp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.wavefront.agent.preprocessor.PreprocessorRuleMetrics;
import com.wavefront.agent.preprocessor.ReportPointAddTagIfNotExistsTransformer;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import wavefront.report.Annotation;
import wavefront.report.HistogramType;
import wavefront.report.ReportPoint;

import static com.wavefront.agent.listeners.otlp.OtlpProtobufPointUtils.MILLIS_IN_DAY;
import static com.wavefront.agent.listeners.otlp.OtlpProtobufPointUtils.MILLIS_IN_HOUR;
import static com.wavefront.agent.listeners.otlp.OtlpProtobufPointUtils.MILLIS_IN_MINUTE;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.DEFAULT_SOURCE;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.assertAllPointsEqual;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.attribute;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.justThePointsNamed;
import static org.junit.Assert.assertEquals;

/**
 * @author Sumit Deo (deosu@vmware.com)
 */
public class OtlpProtobufPointUtilsTest {
  private final static List<KeyValue> emptyAttrs = Collections.unmodifiableList(new ArrayList<>());
  private static final long startTimeMs = System.currentTimeMillis();

  private List<ReportPoint> actualPoints;
  private ImmutableList<ReportPoint> expectedPoints;

  @Test
  public void rejectsEmptyMetric() {
    Metric otlpMetric = OtlpTestHelpers.otlpMetricGenerator().build();

    Assert.assertThrows(IllegalArgumentException.class, () -> {
      OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);
    });
  }

  @Test
  public void rejectsGaugeWithZeroDataPoints() {
    Gauge emptyGauge = Gauge.newBuilder().build();
    Metric otlpMetric = OtlpTestHelpers.otlpMetricGenerator().setGauge(emptyGauge).build();

    Assert.assertThrows(IllegalArgumentException.class, () -> {
      OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);
    });
  }

  @Test
  public void transformsMinimalGauge() {
    Gauge otlpGauge = Gauge.newBuilder().addDataPoints(NumberDataPoint.newBuilder().build()).build();
    Metric otlpMetric = OtlpTestHelpers.otlpMetricGenerator().setGauge(otlpGauge).build();
    expectedPoints = ImmutableList.of(OtlpTestHelpers.wfReportPointGenerator().build());
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void transformsGaugeTimestampToEpochMilliseconds() {
    long timeInNanos = TimeUnit.MILLISECONDS.toNanos(startTimeMs);
    Gauge otlpGauge = Gauge.newBuilder().addDataPoints(NumberDataPoint.newBuilder().setTimeUnixNano(timeInNanos).build()).build();
    Metric otlpMetric = OtlpTestHelpers.otlpMetricGenerator().setGauge(otlpGauge).build();
    expectedPoints = ImmutableList.of(OtlpTestHelpers.wfReportPointGenerator().setTimestamp(startTimeMs).build());
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void acceptsGaugeWithMultipleDataPoints() {
    List<NumberDataPoint> points = ImmutableList.of(
        NumberDataPoint.newBuilder().setTimeUnixNano(TimeUnit.SECONDS.toNanos(1)).setAsDouble(1.0).build(),
        NumberDataPoint.newBuilder().setTimeUnixNano(TimeUnit.SECONDS.toNanos(2)).setAsDouble(2.0).build()
    );
    Metric otlpMetric = OtlpTestHelpers.otlpGaugeGenerator(points).build();

    expectedPoints = ImmutableList.of(
        OtlpTestHelpers.wfReportPointGenerator().setTimestamp(TimeUnit.SECONDS.toMillis(1)).setValue(1.0).build(),
        OtlpTestHelpers.wfReportPointGenerator().setTimestamp(TimeUnit.SECONDS.toMillis(2)).setValue(2.0).build()
    );
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void handlesGaugeAttributes() {
    KeyValue booleanAttr = KeyValue.newBuilder().setKey("a-boolean")
        .setValue(AnyValue.newBuilder().setBoolValue(true).build())
        .build();

    Gauge otlpGauge = Gauge.newBuilder().addDataPoints(NumberDataPoint.newBuilder().addAttributes(booleanAttr).build())
        .build();
    Metric otlpMetric = OtlpTestHelpers.otlpMetricGenerator().setGauge(otlpGauge).build();

    List<Annotation> wfAttrs = Collections.singletonList(
        Annotation.newBuilder().setKey("a-boolean").setValue("true").build()
    );
    expectedPoints = ImmutableList.of(OtlpTestHelpers.wfReportPointGenerator(wfAttrs).build());
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void rejectsSumWithZeroDataPoints() {
    Sum emptySum = Sum.newBuilder().build();
    Metric otlpMetric = OtlpTestHelpers.otlpMetricGenerator().setSum(emptySum).build();

    Assert.assertThrows(IllegalArgumentException.class, () -> {
      OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);
    });
  }

  @Test
  public void transformsMinimalSum() {
    Sum otlpSum = Sum.newBuilder()
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
        .addDataPoints(NumberDataPoint.newBuilder().build())
        .build();
    Metric otlpMetric = OtlpTestHelpers.otlpMetricGenerator().setSum(otlpSum).build();
    expectedPoints = ImmutableList.of(OtlpTestHelpers.wfReportPointGenerator().build());
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void transformsSumTimestampToEpochMilliseconds() {
    long timeInNanos = TimeUnit.MILLISECONDS.toNanos(startTimeMs);
    Sum otlpSum = Sum.newBuilder()
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
        .addDataPoints(NumberDataPoint.newBuilder().setTimeUnixNano(timeInNanos).build())
        .build();
    Metric otlpMetric = OtlpTestHelpers.otlpMetricGenerator().setSum(otlpSum).build();
    expectedPoints = ImmutableList.of(OtlpTestHelpers.wfReportPointGenerator().setTimestamp(startTimeMs).build());
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void acceptsSumWithMultipleDataPoints() {
    List<NumberDataPoint> points = ImmutableList.of(
        NumberDataPoint.newBuilder().setTimeUnixNano(TimeUnit.SECONDS.toNanos(1)).setAsDouble(1.0).build(),
        NumberDataPoint.newBuilder().setTimeUnixNano(TimeUnit.SECONDS.toNanos(2)).setAsDouble(2.0).build()
    );
    Metric otlpMetric = OtlpTestHelpers.otlpSumGenerator(points).build();

    expectedPoints = ImmutableList.of(
        OtlpTestHelpers.wfReportPointGenerator().setTimestamp(TimeUnit.SECONDS.toMillis(1)).setValue(1.0).build(),
        OtlpTestHelpers.wfReportPointGenerator().setTimestamp(TimeUnit.SECONDS.toMillis(2)).setValue(2.0).build()
    );
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void handlesSumAttributes() {
    KeyValue booleanAttr = KeyValue.newBuilder().setKey("a-boolean")
        .setValue(AnyValue.newBuilder().setBoolValue(true).build())
        .build();

    Sum otlpSum = Sum.newBuilder()
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
        .addDataPoints(NumberDataPoint.newBuilder().addAttributes(booleanAttr).build())
        .build();
    Metric otlpMetric = OtlpTestHelpers.otlpMetricGenerator().setSum(otlpSum).build();

    List<Annotation> wfAttrs = Collections.singletonList(
        Annotation.newBuilder().setKey("a-boolean").setValue("true").build()
    );
    expectedPoints = ImmutableList.of(OtlpTestHelpers.wfReportPointGenerator(wfAttrs).build());
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void addsPrefixToDeltaSums() {
    Sum otlpSum = Sum.newBuilder()
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
        .addDataPoints(NumberDataPoint.newBuilder().build())
        .build();
    Metric otlpMetric = OtlpTestHelpers.otlpMetricGenerator().setSum(otlpSum).setName("testSum").build();
    ReportPoint reportPoint = OtlpTestHelpers.wfReportPointGenerator().setMetric("∆testSum").build();
    expectedPoints = ImmutableList.of(reportPoint);
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void transformsMinimalSummary() {
    SummaryDataPoint point = SummaryDataPoint.newBuilder()
        .addQuantileValues(SummaryDataPoint.ValueAtQuantile.newBuilder()
            .setQuantile(.5)
            .setValue(12.3)
            .build())
        .setSum(24.5)
        .setCount(3)
        .build();
    Metric otlpMetric = OtlpTestHelpers.otlpSummaryGenerator(point).setName("testSummary").build();

    expectedPoints = ImmutableList.of(
        OtlpTestHelpers.wfReportPointGenerator().setMetric("testSummary_sum").setValue(24.5).build(),
        OtlpTestHelpers.wfReportPointGenerator().setMetric("testSummary_count").setValue(3).build(),
        OtlpTestHelpers.wfReportPointGenerator().setMetric("testSummary").setValue(12.3).setAnnotations(ImmutableMap.of("quantile", "0.5")).build()
    );
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void transformsSummaryTimestampToEpochMilliseconds() {
    SummaryDataPoint point = SummaryDataPoint.newBuilder()
        .addQuantileValues(SummaryDataPoint.ValueAtQuantile.newBuilder().build())
        .setTimeUnixNano(TimeUnit.MILLISECONDS.toNanos(startTimeMs))
        .build();
    Metric otlpMetric = OtlpTestHelpers.otlpSummaryGenerator(point).build();
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    for (ReportPoint p : actualPoints) {
      assertEquals(startTimeMs, p.getTimestamp());
    }
  }

  @Test
  public void acceptsSummaryWithMultipleDataPoints() {
    List<SummaryDataPoint> points = ImmutableList.of(
        SummaryDataPoint.newBuilder().setTimeUnixNano(TimeUnit.SECONDS.toNanos(1)).setSum(1.0).setCount(1).build(),
        SummaryDataPoint.newBuilder().setTimeUnixNano(TimeUnit.SECONDS.toNanos(2)).setSum(2.0).setCount(2).build()
    );
    Summary otlpSummary = Summary.newBuilder().addAllDataPoints(points).build();
    Metric otlpMetric = OtlpTestHelpers.otlpMetricGenerator().setSummary(otlpSummary).build();

    expectedPoints = ImmutableList.of(
        // SummaryDataPoint 1
        OtlpTestHelpers.wfReportPointGenerator().setMetric("test_sum").setTimestamp(TimeUnit.SECONDS.toMillis(1)).setValue(1.0).build(),
        OtlpTestHelpers.wfReportPointGenerator().setMetric("test_count").setTimestamp(TimeUnit.SECONDS.toMillis(1)).setValue(1).build(),
        // SummaryDataPoint 2
        OtlpTestHelpers.wfReportPointGenerator().setMetric("test_sum").setTimestamp(TimeUnit.SECONDS.toMillis(2)).setValue(2.0).build(),
        OtlpTestHelpers.wfReportPointGenerator().setMetric("test_count").setTimestamp(TimeUnit.SECONDS.toMillis(2)).setValue(2).build()
    );
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void createsMetricsForEachSummaryQuantile() {
    Metric otlpMetric = OtlpTestHelpers.otlpSummaryGenerator(ImmutableList.of(
        SummaryDataPoint.ValueAtQuantile.newBuilder()
            .setQuantile(.2)
            .setValue(2.2)
            .build(),
        SummaryDataPoint.ValueAtQuantile.newBuilder()
            .setQuantile(.4)
            .setValue(4.4)
            .build(),
        SummaryDataPoint.ValueAtQuantile.newBuilder()
            .setQuantile(.6)
            .setValue(6.6)
            .build()
    )).build();

    expectedPoints = ImmutableList.of(
        OtlpTestHelpers.wfReportPointGenerator()
            .setAnnotations(ImmutableMap.of("quantile", "0.2"))
            .setValue(2.2)
            .build(),
        OtlpTestHelpers.wfReportPointGenerator()
            .setAnnotations(ImmutableMap.of("quantile", "0.4"))
            .setValue(4.4)
            .build(),
        OtlpTestHelpers.wfReportPointGenerator()
            .setAnnotations(ImmutableMap.of("quantile", "0.6"))
            .setValue(6.6)
            .build()
    );
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, justThePointsNamed("test", actualPoints));
  }

  @Test
  public void preservesOverriddenQuantileTag() {
    KeyValue quantileTag = KeyValue.newBuilder()
        .setKey("quantile")
        .setValue(AnyValue.newBuilder().setStringValue("half").build())
        .build();
    SummaryDataPoint point = SummaryDataPoint.newBuilder()
        .addQuantileValues(SummaryDataPoint.ValueAtQuantile.newBuilder()
            .setQuantile(.5)
            .setValue(12.3)
            .build())
        .addAttributes(quantileTag)
        .build();
    Metric otlpMetric = OtlpTestHelpers.otlpSummaryGenerator(point).setName("testSummary").build();

    for (ReportPoint p : OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE)) {
      assertEquals("half", p.getAnnotations().get("_quantile"));
      if (p.getMetric().equals("testSummary")) {
        assertEquals("0.5", p.getAnnotations().get("quantile"));
      }
    }
  }


  @Test
  public void handlesSummaryAttributes() {
    KeyValue booleanAttr = KeyValue.newBuilder().setKey("a-boolean")
        .setValue(AnyValue.newBuilder().setBoolValue(true).build())
        .build();

    SummaryDataPoint dataPoint = SummaryDataPoint.newBuilder().addAttributes(booleanAttr).build();
    Metric otlpMetric = OtlpTestHelpers.otlpSummaryGenerator(dataPoint).build();

    for (ReportPoint p : OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE)) {
      assertEquals("true", p.getAnnotations().get("a-boolean"));
    }
  }

  @Test
  public void transformsMinimalCumulativeHistogram() {
    HistogramDataPoint point = HistogramDataPoint.newBuilder()
        .addAllExplicitBounds(ImmutableList.of(1.0, 2.0))
        .addAllBucketCounts(ImmutableList.of(1L, 1L, 1L))
        .build();
    Histogram histo = Histogram.newBuilder()
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
        .addAllDataPoints(Collections.singletonList(point)).build();

    Metric otlpMetric =
        OtlpTestHelpers.otlpMetricGenerator().setHistogram(histo).build();
    expectedPoints = ImmutableList.of(
        OtlpTestHelpers.wfReportPointGenerator(ImmutableList.of(new Annotation("le", "1.0")))
            .setValue(1).build(),
        OtlpTestHelpers.wfReportPointGenerator(ImmutableList.of(new Annotation("le", "2.0")))
            .setValue(2).build(),
        OtlpTestHelpers.wfReportPointGenerator(ImmutableList.of(new Annotation("le", "+Inf")))
            .setValue(3).build()
    );
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void transformsCumulativeHistogramWithoutBounds() {
    HistogramDataPoint point = HistogramDataPoint.newBuilder()
        .addAllBucketCounts(ImmutableList.of(1L))
        .build();
    Histogram histo = Histogram.newBuilder()
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
        .addAllDataPoints(Collections.singletonList(point)).build();

    Metric otlpMetric =
        OtlpTestHelpers.otlpMetricGenerator().setHistogram(histo).build();
    expectedPoints = ImmutableList.of(
        OtlpTestHelpers.wfReportPointGenerator(ImmutableList.of(new Annotation("le", "+Inf")))
            .setValue(1).build()
    );
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void transformsCumulativeHistogramWithTagLe() {
    HistogramDataPoint point = HistogramDataPoint.newBuilder()
        .addAllBucketCounts(ImmutableList.of(1L))
        .addAttributes(OtlpTestHelpers.attribute("le", "someVal"))
        .build();
    Histogram histo = Histogram.newBuilder()
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
        .addAllDataPoints(Collections.singletonList(point)).build();

    Metric otlpMetric =
        OtlpTestHelpers.otlpMetricGenerator().setHistogram(histo).build();
    expectedPoints = ImmutableList.of(
        OtlpTestHelpers.wfReportPointGenerator(ImmutableList.of(new Annotation("le", "+Inf"),
                new Annotation("_le", "someVal")))
            .setValue(1).build()
    );
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void transformsCumulativeHistogramThrowsMalformedDataPointsError() {
    HistogramDataPoint point = HistogramDataPoint.newBuilder()
        .addAllExplicitBounds(Collections.singletonList(1.0))
        .addAllBucketCounts(ImmutableList.of(1L))
        .build();
    Histogram histo = Histogram.newBuilder()
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
        .addAllDataPoints(Collections.singletonList(point)).build();

    Metric otlpMetric =
        OtlpTestHelpers.otlpMetricGenerator().setHistogram(histo).build();

    Assert.assertThrows(IllegalArgumentException.class,
        () -> OtlpProtobufPointUtils.transform(otlpMetric,
            emptyAttrs, null, DEFAULT_SOURCE));
  }

  @Test
  public void transformsMinimalDeltaHistogram() {
    HistogramDataPoint point = HistogramDataPoint.newBuilder()
        .addAllExplicitBounds(ImmutableList.of(1.0, 2.0))
        .addAllBucketCounts(ImmutableList.of(1L, 2L, 3L))
        .build();
    Histogram histo = Histogram.newBuilder()
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
        .addAllDataPoints(Collections.singletonList(point)).build();

    Metric otlpMetric =
        OtlpTestHelpers.otlpMetricGenerator().setHistogram(histo).build();

    List<Double> bins = new ArrayList<>(Arrays.asList(1.0, 1.5, 2.0));
    List<Integer> counts = new ArrayList<>(Arrays.asList(1, 2, 3));

    wavefront.report.Histogram minHistogram = wavefront.report.Histogram.newBuilder().
        setType(HistogramType.TDIGEST).
        setBins(bins).
        setCounts(counts).
        setDuration(MILLIS_IN_MINUTE).
        build();

    wavefront.report.Histogram hourHistogram = wavefront.report.Histogram.newBuilder().
        setType(HistogramType.TDIGEST).
        setBins(bins).
        setCounts(counts).
        setDuration(MILLIS_IN_HOUR).
        build();

    wavefront.report.Histogram dayHistogram = wavefront.report.Histogram.newBuilder().
        setType(HistogramType.TDIGEST).
        setBins(bins).
        setCounts(counts).
        setDuration(MILLIS_IN_DAY).
        build();

    expectedPoints = ImmutableList.of(
        OtlpTestHelpers.wfReportPointGenerator()
            .setValue(minHistogram).build(),
        OtlpTestHelpers.wfReportPointGenerator()
            .setValue(hourHistogram).build(),
        OtlpTestHelpers.wfReportPointGenerator()
            .setValue(dayHistogram).build()
    );

    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void transformsDeltaHistogramWithoutBounds() {
    HistogramDataPoint point = HistogramDataPoint.newBuilder()
        .addAllBucketCounts(ImmutableList.of(1L))
        .build();
    Histogram histo = Histogram.newBuilder()
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
        .addAllDataPoints(Collections.singletonList(point)).build();

    Metric otlpMetric =
        OtlpTestHelpers.otlpMetricGenerator().setHistogram(histo).build();

    List<Double> bins = new ArrayList<>(Collections.singletonList(0.0));
    List<Integer> counts = new ArrayList<>(Collections.singletonList(1));

    wavefront.report.Histogram minHistogram = wavefront.report.Histogram.newBuilder().
        setType(HistogramType.TDIGEST).
        setBins(bins).
        setCounts(counts).
        setDuration(MILLIS_IN_MINUTE).
        build();

    wavefront.report.Histogram hourHistogram = wavefront.report.Histogram.newBuilder().
        setType(HistogramType.TDIGEST).
        setBins(bins).
        setCounts(counts).
        setDuration(MILLIS_IN_HOUR).
        build();

    wavefront.report.Histogram dayHistogram = wavefront.report.Histogram.newBuilder().
        setType(HistogramType.TDIGEST).
        setBins(bins).
        setCounts(counts).
        setDuration(MILLIS_IN_DAY).
        build();

    expectedPoints = ImmutableList.of(
        OtlpTestHelpers.wfReportPointGenerator()
            .setValue(minHistogram).build(),
        OtlpTestHelpers.wfReportPointGenerator()
            .setValue(hourHistogram).build(),
        OtlpTestHelpers.wfReportPointGenerator()
            .setValue(dayHistogram).build()
    );

    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void transformsDeltaHistogramThrowsMalformedDataPointsError() {
    HistogramDataPoint point = HistogramDataPoint.newBuilder()
        .addAllExplicitBounds(Collections.singletonList(1.0))
        .addAllBucketCounts(ImmutableList.of(1L))
        .build();
    Histogram histo = Histogram.newBuilder()
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
        .addAllDataPoints(Collections.singletonList(point)).build();

    Metric otlpMetric =
        OtlpTestHelpers.otlpMetricGenerator().setHistogram(histo).build();

    Assert.assertThrows(IllegalArgumentException.class,
        () -> OtlpProtobufPointUtils.transform(otlpMetric,
            emptyAttrs, null, DEFAULT_SOURCE));
  }

  @Test
  public void convertsResourceAttributesToAnnotations() {
    List<KeyValue> resourceAttrs = Collections.singletonList(attribute("r-key", "r-value"));
    expectedPoints = ImmutableList.of(OtlpTestHelpers.wfReportPointGenerator(
        Collections.singletonList(new Annotation("r-key", "r-value"))
    ).build());
    NumberDataPoint point = NumberDataPoint.newBuilder().setTimeUnixNano(0).build();
    Metric otlpMetric = OtlpTestHelpers.otlpGaugeGenerator(point).build();

    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, resourceAttrs, null, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }

  @Test
  public void dataPointAttributesHaveHigherPrecedenceThanResourceAttributes() {
    String key = "the-key";
    NumberDataPoint point = NumberDataPoint.newBuilder().addAttributes(attribute(key, "gauge-value")).build();
    Metric otlpMetric = OtlpTestHelpers.otlpGaugeGenerator(point).build();
    List<KeyValue> resourceAttrs = Collections.singletonList(attribute(key, "rsrc-value"));

    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, resourceAttrs, null, DEFAULT_SOURCE);

    assertEquals("gauge-value", actualPoints.get(0).getAnnotations().get(key));
  }

  @Test
  public void setsSource() {
    Metric otlpMetric =
        OtlpTestHelpers.otlpGaugeGenerator(NumberDataPoint.newBuilder().build()).build();
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, null, "a-src");

    assertEquals("a-src", actualPoints.get(0).getHost());
  }

  @Test
  public void appliesPreprocessorRules() {
    List<NumberDataPoint> dataPoints = Collections.singletonList(NumberDataPoint.newBuilder().setTimeUnixNano(0).build());
    Metric otlpMetric = OtlpTestHelpers.otlpGaugeGenerator(dataPoints).build();
    List<Annotation> wfAttrs = Collections.singletonList(
        Annotation.newBuilder().setKey("my-key").setValue("my-value").build()
    );
    ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
    PreprocessorRuleMetrics preprocessorRuleMetrics = new PreprocessorRuleMetrics(null, null,
        null);
    for (Annotation annotation : wfAttrs) {
      preprocessor.forReportPoint().addTransformer(new ReportPointAddTagIfNotExistsTransformer(
          annotation.getKey(), annotation.getValue(), x -> true, preprocessorRuleMetrics));
    }
    expectedPoints = ImmutableList.of(OtlpTestHelpers.wfReportPointGenerator(wfAttrs).build());
    actualPoints = OtlpProtobufPointUtils.transform(otlpMetric, emptyAttrs, preprocessor, DEFAULT_SOURCE);

    assertAllPointsEqual(expectedPoints, actualPoints);
  }
}