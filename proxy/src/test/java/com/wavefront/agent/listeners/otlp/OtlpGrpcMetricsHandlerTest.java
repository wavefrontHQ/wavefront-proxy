package com.wavefront.agent.listeners.otlp;

import static com.wavefront.agent.listeners.otlp.OtlpMetricsUtils.MILLIS_IN_DAY;
import static com.wavefront.agent.listeners.otlp.OtlpMetricsUtils.MILLIS_IN_HOUR;
import static com.wavefront.agent.listeners.otlp.OtlpMetricsUtils.MILLIS_IN_MINUTE;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.DEFAULT_SOURCE;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.api.agent.preprocessor.ReportableEntityPreprocessor;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Histogram;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import wavefront.report.HistogramType;
import wavefront.report.ReportPoint;

public class OtlpGrpcMetricsHandlerTest {

  public static final StreamObserver<ExportMetricsServiceResponse> emptyStreamObserver =
      new StreamObserver<ExportMetricsServiceResponse>() {

        @Override
        public void onNext(ExportMetricsServiceResponse exportMetricsServiceResponse) {}

        @Override
        public void onError(Throwable throwable) {}

        @Override
        public void onCompleted() {}
      };

  private final ReportableEntityHandler<ReportPoint, String> mockReportPointHandler =
      MockReportableEntityHandlerFactory.getMockReportPointHandler();
  private final ReportableEntityHandler<ReportPoint, String> mockHistogramHandler =
      MockReportableEntityHandlerFactory.getMockReportPointHandler();
  private OtlpGrpcMetricsHandler subject;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier =
      ReportableEntityPreprocessor::new;

  @Before
  public void setup() {
    subject =
        new OtlpGrpcMetricsHandler(
            mockReportPointHandler,
            mockHistogramHandler,
            preprocessorSupplier,
            DEFAULT_SOURCE,
            false,
            true);
  }

  @Test
  public void simpleGauge() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockReportPointHandler);
    Gauge otelGauge =
        Gauge.newBuilder()
            .addDataPoints(
                NumberDataPoint.newBuilder()
                    .setAsDouble(12.3)
                    .setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime))
                    .build())
            .build();
    Metric otelMetric = Metric.newBuilder().setGauge(otelGauge).setName("test-gauge").build();
    wavefront.report.ReportPoint wfMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-gauge")
            .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
            .setValue(12.3)
            .setHost(DEFAULT_SOURCE)
            .build();
    mockReportPointHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void monotonicCumulativeSum() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockReportPointHandler);
    Sum otelSum =
        Sum.newBuilder()
            .setIsMonotonic(true)
            .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
            .addDataPoints(
                NumberDataPoint.newBuilder()
                    .setAsDouble(12.3)
                    .setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime))
                    .build())
            .build();
    Metric otelMetric = Metric.newBuilder().setSum(otelSum).setName("test-sum").build();
    wavefront.report.ReportPoint wfMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-sum")
            .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
            .setValue(12.3)
            .setHost(DEFAULT_SOURCE)
            .build();
    mockReportPointHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void nonmonotonicCumulativeSum() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockReportPointHandler);
    Sum otelSum =
        Sum.newBuilder()
            .setIsMonotonic(false)
            .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
            .addDataPoints(
                NumberDataPoint.newBuilder()
                    .setAsDouble(12.3)
                    .setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime))
                    .build())
            .build();
    Metric otelMetric = Metric.newBuilder().setSum(otelSum).setName("test-sum").build();
    wavefront.report.ReportPoint wfMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-sum")
            .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
            .setValue(12.3)
            .setHost(DEFAULT_SOURCE)
            .build();
    mockReportPointHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void simpleSummary() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockReportPointHandler);
    SummaryDataPoint point =
        SummaryDataPoint.newBuilder()
            .setSum(12.3)
            .setCount(21)
            .addQuantileValues(
                SummaryDataPoint.ValueAtQuantile.newBuilder()
                    .setQuantile(.5)
                    .setValue(242.3)
                    .build())
            .setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime))
            .build();
    Summary otelSummary = Summary.newBuilder().addDataPoints(point).build();
    Metric otelMetric = Metric.newBuilder().setSummary(otelSummary).setName("test-summary").build();
    mockReportPointHandler.report(
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-summary_sum")
            .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
            .setValue(12.3)
            .setHost(DEFAULT_SOURCE)
            .build());
    EasyMock.expectLastCall();
    mockReportPointHandler.report(
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-summary_count")
            .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
            .setValue(21)
            .setHost(DEFAULT_SOURCE)
            .build());
    EasyMock.expectLastCall();
    mockReportPointHandler.report(
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-summary")
            .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
            .setValue(242.3)
            .setAnnotations(ImmutableMap.of("quantile", "0.5"))
            .setHost(DEFAULT_SOURCE)
            .build());
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void monotonicDeltaSum() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockReportPointHandler);
    Sum otelSum =
        Sum.newBuilder()
            .setIsMonotonic(true)
            .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
            .addDataPoints(
                NumberDataPoint.newBuilder()
                    .setAsDouble(12.3)
                    .setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime))
                    .build())
            .build();
    Metric otelMetric = Metric.newBuilder().setSum(otelSum).setName("test-sum").build();
    wavefront.report.ReportPoint wfMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("∆test-sum")
            .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
            .setValue(12.3)
            .setHost(DEFAULT_SOURCE)
            .build();
    mockReportPointHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void nonmonotonicDeltaSum() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockReportPointHandler);
    Sum otelSum =
        Sum.newBuilder()
            .setIsMonotonic(false)
            .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
            .addDataPoints(
                NumberDataPoint.newBuilder()
                    .setAsDouble(12.3)
                    .setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime))
                    .build())
            .build();
    Metric otelMetric = Metric.newBuilder().setSum(otelSum).setName("test-sum").build();
    wavefront.report.ReportPoint wfMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("∆test-sum")
            .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
            .setValue(12.3)
            .setHost(DEFAULT_SOURCE)
            .build();
    mockReportPointHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void setsSourceFromResourceAttributesNotPointAttributes() {
    EasyMock.reset(mockReportPointHandler);

    Map<String, String> annotations = new HashMap<>();
    annotations.put("_source", "at-point");

    ReportPoint wfMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setAnnotations(annotations)
            .setHost("at-resrc")
            .build();
    mockReportPointHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    NumberDataPoint otelPoint =
        NumberDataPoint.newBuilder()
            .addAttributes(OtlpTestHelpers.attribute("source", "at-point"))
            .build();
    Metric otelMetric = OtlpTestHelpers.otlpGaugeGenerator(otelPoint).build();

    Resource otelResource =
        Resource.newBuilder()
            .addAttributes(OtlpTestHelpers.attribute("source", "at-resrc"))
            .build();
    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .setResource(otelResource)
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void cumulativeHistogram() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockReportPointHandler);

    HistogramDataPoint point =
        HistogramDataPoint.newBuilder()
            .addAllExplicitBounds(ImmutableList.of(1.0, 2.0))
            .addAllBucketCounts(ImmutableList.of(1L, 2L, 3L))
            .setTimeUnixNano(epochTime)
            .build();

    Histogram otelHistogram =
        Histogram.newBuilder()
            .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
            .addAllDataPoints(Collections.singletonList(point))
            .build();

    Metric otelMetric =
        Metric.newBuilder()
            .setHistogram(otelHistogram)
            .setName("test-cumulative-histogram")
            .build();

    wavefront.report.ReportPoint wfMetric1 =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-cumulative-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(1)
            .setHost(DEFAULT_SOURCE)
            .setAnnotations(Collections.singletonMap("le", "1.0"))
            .build();

    wavefront.report.ReportPoint wfMetric2 =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-cumulative-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(3)
            .setHost(DEFAULT_SOURCE)
            .setAnnotations(Collections.singletonMap("le", "2.0"))
            .build();

    wavefront.report.ReportPoint wfMetric3 =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-cumulative-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(6)
            .setHost(DEFAULT_SOURCE)
            .setAnnotations(Collections.singletonMap("le", "+Inf"))
            .build();

    mockReportPointHandler.report(wfMetric1);
    EasyMock.expectLastCall().once();

    mockReportPointHandler.report(wfMetric2);
    EasyMock.expectLastCall().once();

    mockReportPointHandler.report(wfMetric3);
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockReportPointHandler);

    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void deltaHistogram() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockHistogramHandler);

    HistogramDataPoint point =
        HistogramDataPoint.newBuilder()
            .addAllExplicitBounds(ImmutableList.of(1.0, 2.0, 3.0))
            .addAllBucketCounts(ImmutableList.of(1L, 2L, 3L, 4L))
            .setTimeUnixNano(epochTime)
            .build();

    Histogram otelHistogram =
        Histogram.newBuilder()
            .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
            .addAllDataPoints(Collections.singletonList(point))
            .build();

    Metric otelMetric =
        Metric.newBuilder().setHistogram(otelHistogram).setName("test-delta-histogram").build();

    List<Double> bins = new ArrayList<>(Arrays.asList(1.0, 1.5, 2.5, 3.0));
    List<Integer> counts = new ArrayList<>(Arrays.asList(1, 2, 3, 4));

    wavefront.report.Histogram minHistogram =
        wavefront.report.Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setBins(bins)
            .setCounts(counts)
            .setDuration(MILLIS_IN_MINUTE)
            .build();

    wavefront.report.ReportPoint minWFMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-delta-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(minHistogram)
            .setHost(DEFAULT_SOURCE)
            .build();

    wavefront.report.Histogram hourHistogram =
        wavefront.report.Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setBins(bins)
            .setCounts(counts)
            .setDuration(MILLIS_IN_HOUR)
            .build();

    wavefront.report.ReportPoint hourWFMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-delta-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(hourHistogram)
            .setHost(DEFAULT_SOURCE)
            .build();

    wavefront.report.Histogram dayHistogram =
        wavefront.report.Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setBins(bins)
            .setCounts(counts)
            .setDuration(MILLIS_IN_DAY)
            .build();

    wavefront.report.ReportPoint dayWFMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-delta-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(dayHistogram)
            .setHost(DEFAULT_SOURCE)
            .build();

    mockHistogramHandler.report(minWFMetric);
    EasyMock.expectLastCall().once();

    mockHistogramHandler.report(hourWFMetric);
    EasyMock.expectLastCall().once();

    mockHistogramHandler.report(dayWFMetric);
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockHistogramHandler);

    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockHistogramHandler);
  }

  @Test
  public void resourceAttrsCanBeExcluded() {
    String resourceAttrKey = "testKey";

    EasyMock.reset(mockReportPointHandler);

    ReportPoint wfMetric = OtlpTestHelpers.wfReportPointGenerator().build();
    assertFalse(wfMetric.getAnnotations().containsKey(resourceAttrKey));
    mockReportPointHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    NumberDataPoint otelPoint = NumberDataPoint.newBuilder().build();
    Metric otelMetric = OtlpTestHelpers.otlpGaugeGenerator(otelPoint).build();
    Resource resource =
        Resource.newBuilder()
            .addAttributes(OtlpTestHelpers.attribute(resourceAttrKey, "testValue"))
            .build();
    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .setResource(resource)
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();

    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void resourceAttrsCanBeIncluded() {
    boolean includeResourceAttrsForMetrics = true;
    subject =
        new OtlpGrpcMetricsHandler(
            mockReportPointHandler,
            mockHistogramHandler,
            preprocessorSupplier,
            DEFAULT_SOURCE,
            includeResourceAttrsForMetrics,
            true);
    String resourceAttrKey = "testKey";

    EasyMock.reset(mockReportPointHandler);

    Map<String, String> annotations = ImmutableMap.of(resourceAttrKey, "testValue");
    ReportPoint wfMetric =
        OtlpTestHelpers.wfReportPointGenerator().setAnnotations(annotations).build();
    mockReportPointHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    NumberDataPoint otelPoint = NumberDataPoint.newBuilder().build();
    Metric otelMetric = OtlpTestHelpers.otlpGaugeGenerator(otelPoint).build();
    Resource resource =
        Resource.newBuilder()
            .addAttributes(OtlpTestHelpers.attribute(resourceAttrKey, "testValue"))
            .build();
    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .setResource(resource)
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void resourceAttrsWithServiceNameCanBeIncluded() {
    boolean includeResourceAttrsForMetrics = true;
    subject =
        new OtlpGrpcMetricsHandler(
            mockReportPointHandler,
            mockHistogramHandler,
            preprocessorSupplier,
            DEFAULT_SOURCE,
            includeResourceAttrsForMetrics,
            true);

    EasyMock.reset(mockReportPointHandler);

    Map<String, String> annotations = ImmutableMap.of("service", "testValue");
    ReportPoint wfMetric =
        OtlpTestHelpers.wfReportPointGenerator().setAnnotations(annotations).build();
    mockReportPointHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    NumberDataPoint otelPoint = NumberDataPoint.newBuilder().build();
    Metric otelMetric = OtlpTestHelpers.otlpGaugeGenerator(otelPoint).build();
    Resource resource =
        Resource.newBuilder()
            .addAttributes(OtlpTestHelpers.attribute("service.name", "testValue"))
            .build();
    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .setResource(resource)
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void exponentialDeltaHistogram() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockHistogramHandler);

    ExponentialHistogramDataPoint point =
        ExponentialHistogramDataPoint.newBuilder()
            .setScale(0)
            .setTimeUnixNano(epochTime)
            .setPositive(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                    .setOffset(2)
                    .addBucketCounts(1)
                    .build())
            .setZeroCount(2)
            .setNegative(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                    .setOffset(2)
                    .addBucketCounts(3)
                    .build())
            .build();

    ExponentialHistogram otelHistogram =
        ExponentialHistogram.newBuilder()
            .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
            .addDataPoints(point)
            .build();

    Metric otelMetric =
        Metric.newBuilder()
            .setExponentialHistogram(otelHistogram)
            .setName("test-exp-delta-histogram")
            .build();

    List<Double> bins = new ArrayList<>(Arrays.asList(-6.0, 0.0, 6.0));
    List<Integer> counts = new ArrayList<>(Arrays.asList(3, 2, 1));

    wavefront.report.Histogram minHistogram =
        wavefront.report.Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setBins(bins)
            .setCounts(counts)
            .setDuration(MILLIS_IN_MINUTE)
            .build();

    wavefront.report.ReportPoint minWFMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-exp-delta-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(minHistogram)
            .setHost(DEFAULT_SOURCE)
            .build();

    wavefront.report.Histogram hourHistogram =
        wavefront.report.Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setBins(bins)
            .setCounts(counts)
            .setDuration(MILLIS_IN_HOUR)
            .build();

    wavefront.report.ReportPoint hourWFMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-exp-delta-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(hourHistogram)
            .setHost(DEFAULT_SOURCE)
            .build();

    wavefront.report.Histogram dayHistogram =
        wavefront.report.Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setBins(bins)
            .setCounts(counts)
            .setDuration(MILLIS_IN_DAY)
            .build();

    wavefront.report.ReportPoint dayWFMetric =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-exp-delta-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(dayHistogram)
            .setHost(DEFAULT_SOURCE)
            .build();

    mockHistogramHandler.report(minWFMetric);
    EasyMock.expectLastCall().once();

    mockHistogramHandler.report(hourWFMetric);
    EasyMock.expectLastCall().once();

    mockHistogramHandler.report(dayWFMetric);
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockHistogramHandler);

    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockHistogramHandler);
  }

  @Test
  public void exponentialCumulativeHistogram() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockReportPointHandler);

    ExponentialHistogramDataPoint point =
        ExponentialHistogramDataPoint.newBuilder()
            .setScale(0)
            .setTimeUnixNano(epochTime)
            .setPositive(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                    .setOffset(2)
                    .addBucketCounts(1)
                    .build())
            .setZeroCount(2)
            .setNegative(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                    .setOffset(2)
                    .addBucketCounts(3)
                    .build())
            .build();

    ExponentialHistogram otelHistogram =
        ExponentialHistogram.newBuilder()
            .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
            .addDataPoints(point)
            .build();

    Metric otelMetric =
        Metric.newBuilder()
            .setExponentialHistogram(otelHistogram)
            .setName("test-exp-cumulative-histogram")
            .build();

    wavefront.report.ReportPoint wfMetric1 =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-exp-cumulative-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(3)
            .setHost(DEFAULT_SOURCE)
            .setAnnotations(Collections.singletonMap("le", "-4.0"))
            .build();

    wavefront.report.ReportPoint wfMetric2 =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-exp-cumulative-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(5)
            .setHost(DEFAULT_SOURCE)
            .setAnnotations(Collections.singletonMap("le", "4.0"))
            .build();

    wavefront.report.ReportPoint wfMetric3 =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-exp-cumulative-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(6)
            .setHost(DEFAULT_SOURCE)
            .setAnnotations(Collections.singletonMap("le", "8.0"))
            .build();

    wavefront.report.ReportPoint wfMetric4 =
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-exp-cumulative-histogram")
            .setTimestamp(TimeUnit.NANOSECONDS.toMillis(epochTime))
            .setValue(6)
            .setHost(DEFAULT_SOURCE)
            .setAnnotations(Collections.singletonMap("le", "+Inf"))
            .build();

    mockReportPointHandler.report(wfMetric1);
    EasyMock.expectLastCall().once();

    mockReportPointHandler.report(wfMetric2);
    EasyMock.expectLastCall().once();

    mockReportPointHandler.report(wfMetric3);
    EasyMock.expectLastCall().once();

    mockReportPointHandler.report(wfMetric4);
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockReportPointHandler);

    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void appRelatedResAttrsShouldBeIncluded() {
    String applicationKey = "application";
    String serviceKey = "service.name";
    String shardKey = "shard";
    String clusterKey = "cluster";

    EasyMock.reset(mockReportPointHandler);

    Map<String, String> annotations =
        ImmutableMap.<String, String>builder()
            .put(applicationKey, "some-app-name")
            .put("service", "some-service-name")
            .put(shardKey, "some-shard-name")
            .put(clusterKey, "some-cluster-name")
            .build();

    ReportPoint wfMetric =
        OtlpTestHelpers.wfReportPointGenerator().setAnnotations(annotations).build();
    mockReportPointHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    NumberDataPoint otelPoint = NumberDataPoint.newBuilder().build();
    Metric otelMetric = OtlpTestHelpers.otlpGaugeGenerator(otelPoint).build();
    Resource resource =
        Resource.newBuilder()
            .addAttributes(OtlpTestHelpers.attribute(applicationKey, "some-app-name"))
            .addAttributes(OtlpTestHelpers.attribute(serviceKey, "some-service-name"))
            .addAttributes(OtlpTestHelpers.attribute(shardKey, "some-shard-name"))
            .addAttributes(OtlpTestHelpers.attribute(clusterKey, "some-cluster-name"))
            .build();
    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .setResource(resource)
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }

  @Test
  public void appRelatedResAttrsCanBeExcluded() {
    boolean shouldIncludeOtlpAppTagsOnMetrics = false;

    subject =
        new OtlpGrpcMetricsHandler(
            mockReportPointHandler,
            mockHistogramHandler,
            preprocessorSupplier,
            DEFAULT_SOURCE,
            false,
            shouldIncludeOtlpAppTagsOnMetrics);
    String applicationKey = "application";
    String serviceNameKey = "service.name";
    String shardKey = "shard";
    String clusterKey = "cluster";

    EasyMock.reset(mockReportPointHandler);

    ReportPoint wfMetric = OtlpTestHelpers.wfReportPointGenerator().build();
    assertFalse(wfMetric.getAnnotations().containsKey(applicationKey));
    assertFalse(wfMetric.getAnnotations().containsKey(serviceNameKey));
    assertFalse(wfMetric.getAnnotations().containsKey("service"));
    assertFalse(wfMetric.getAnnotations().containsKey(shardKey));
    assertFalse(wfMetric.getAnnotations().containsKey(clusterKey));
    mockReportPointHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockReportPointHandler);

    NumberDataPoint otelPoint = NumberDataPoint.newBuilder().build();
    Metric otelMetric = OtlpTestHelpers.otlpGaugeGenerator(otelPoint).build();
    Resource resource =
        Resource.newBuilder()
            .addAttributes(OtlpTestHelpers.attribute(applicationKey, "some-app-name"))
            .addAttributes(OtlpTestHelpers.attribute(serviceNameKey, "some-service-name"))
            .addAttributes(OtlpTestHelpers.attribute(shardKey, "some-shard-name"))
            .addAttributes(OtlpTestHelpers.attribute(clusterKey, "some-cluster-name"))
            .build();
    ResourceMetrics resourceMetrics =
        ResourceMetrics.newBuilder()
            .setResource(resource)
            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(otelMetric).build())
            .build();
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();

    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockReportPointHandler);
  }
}
