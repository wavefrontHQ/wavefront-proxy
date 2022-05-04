package com.wavefront.agent.listeners.otlp;

import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.metrics.v1.Summary;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import io.opentelemetry.proto.resource.v1.Resource;
import wavefront.report.ReportPoint;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.DEFAULT_SOURCE;

public class OtlpMetricsTest {

  public static final StreamObserver<ExportMetricsServiceResponse> emptyStreamObserver = new StreamObserver<ExportMetricsServiceResponse>() {

    @Override
    public void onNext(ExportMetricsServiceResponse exportMetricsServiceResponse) {
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void onCompleted() {
    }
  };
    private final ReportableEntityHandler<ReportPoint, String> mockMetricsHandler = MockReportableEntityHandlerFactory.getMockReportPointHandler();
  private OtlpGrpcMetricsHandler subject;

  @Before
  public void setup() {
    Supplier<ReportableEntityPreprocessor> preprocessorSupplier = ReportableEntityPreprocessor::new;
    subject = new OtlpGrpcMetricsHandler(mockMetricsHandler, preprocessorSupplier, DEFAULT_SOURCE);
  }

  @Test
  public void simpleGauge() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockMetricsHandler);
    Gauge otelGauge = Gauge.newBuilder()
        .addDataPoints(NumberDataPoint.newBuilder().setAsDouble(12.3).setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime)).build())
        .build();
    Metric otelMetric = Metric.newBuilder()
        .setGauge(otelGauge)
        .setName("test-gauge")
        .build();
    wavefront.report.ReportPoint wfMetric = OtlpTestHelpers.wfReportPointGenerator()
        .setMetric("test-gauge")
        .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
        .setValue(12.3)
        .setHost(DEFAULT_SOURCE)
        .build();
    mockMetricsHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockMetricsHandler);

    ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder().addInstrumentationLibraryMetrics(InstrumentationLibraryMetrics.newBuilder().addMetrics(otelMetric).build()).build();
    ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockMetricsHandler);
  }

  @Test
  public void monotonicCumulativeSum() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockMetricsHandler);
    Sum otelSum = Sum.newBuilder()
        .setIsMonotonic(true)
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
        .addDataPoints(NumberDataPoint.newBuilder().setAsDouble(12.3).setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime)).build())
        .build();
    Metric otelMetric = Metric.newBuilder()
        .setSum(otelSum)
        .setName("test-sum")
        .build();
    wavefront.report.ReportPoint wfMetric = OtlpTestHelpers.wfReportPointGenerator()
        .setMetric("test-sum")
        .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
        .setValue(12.3)
        .setHost(DEFAULT_SOURCE)
        .build();
    mockMetricsHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockMetricsHandler);

    ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder().addInstrumentationLibraryMetrics(InstrumentationLibraryMetrics.newBuilder().addMetrics(otelMetric).build()).build();
    ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockMetricsHandler);
  }

  @Test
  public void nonmonotonicCumulativeSum() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockMetricsHandler);
    Sum otelSum = Sum.newBuilder()
        .setIsMonotonic(false)
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
        .addDataPoints(NumberDataPoint.newBuilder().setAsDouble(12.3).setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime)).build())
        .build();
    Metric otelMetric = Metric.newBuilder()
        .setSum(otelSum)
        .setName("test-sum")
        .build();
    wavefront.report.ReportPoint wfMetric = OtlpTestHelpers.wfReportPointGenerator()
        .setMetric("test-sum")
        .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
        .setValue(12.3)
        .setHost(DEFAULT_SOURCE)
        .build();
    mockMetricsHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockMetricsHandler);

    ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder().addInstrumentationLibraryMetrics(InstrumentationLibraryMetrics.newBuilder().addMetrics(otelMetric).build()).build();
    ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockMetricsHandler);
  }

  @Test
  public void simpleSummary() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockMetricsHandler);
    SummaryDataPoint point = SummaryDataPoint.newBuilder()
        .setSum(12.3)
        .setCount(21)
        .addQuantileValues(SummaryDataPoint.ValueAtQuantile.newBuilder()
            .setQuantile(.5)
            .setValue(242.3)
            .build())
        .setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime)).build();
    Summary otelSummary = Summary.newBuilder()
        .addDataPoints(point)
        .build();
    Metric otelMetric = Metric.newBuilder()
        .setSummary(otelSummary)
        .setName("test-summary")
        .build();
    mockMetricsHandler.report(OtlpTestHelpers.wfReportPointGenerator()
        .setMetric("test-summary_sum")
        .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
        .setValue(12.3)
        .setHost(DEFAULT_SOURCE)
        .build());
    EasyMock.expectLastCall();
    mockMetricsHandler.report(OtlpTestHelpers.wfReportPointGenerator()
        .setMetric("test-summary_count")
        .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
        .setValue(21)
        .setHost(DEFAULT_SOURCE)
        .build());
    EasyMock.expectLastCall();
    mockMetricsHandler.report(OtlpTestHelpers.wfReportPointGenerator()
        .setMetric("test-summary")
        .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
        .setValue(242.3)
        .setAnnotations(ImmutableMap.of("quantile", "0.5"))
        .setHost(DEFAULT_SOURCE)
        .build());
    EasyMock.expectLastCall();

    EasyMock.replay(mockMetricsHandler);

    ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder().addInstrumentationLibraryMetrics(InstrumentationLibraryMetrics.newBuilder().addMetrics(otelMetric).build()).build();
    ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockMetricsHandler);
  }

  @Test
  public void monotonicDeltaSum() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockMetricsHandler);
    Sum otelSum = Sum.newBuilder()
        .setIsMonotonic(true)
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
        .addDataPoints(NumberDataPoint.newBuilder().setAsDouble(12.3).setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime)).build())
        .build();
    Metric otelMetric = Metric.newBuilder()
        .setSum(otelSum)
        .setName("test-sum")
        .build();
    wavefront.report.ReportPoint wfMetric = OtlpTestHelpers.wfReportPointGenerator()
        .setMetric("∆test-sum")
        .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
        .setValue(12.3)
        .setHost(DEFAULT_SOURCE)
        .build();
    mockMetricsHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockMetricsHandler);

    ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder().addInstrumentationLibraryMetrics(InstrumentationLibraryMetrics.newBuilder().addMetrics(otelMetric).build()).build();
    ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockMetricsHandler);
  }

  @Test
  public void nonmonotonicDeltaSum() {
    long epochTime = 1515151515L;
    EasyMock.reset(mockMetricsHandler);
    Sum otelSum = Sum.newBuilder()
        .setIsMonotonic(false)
        .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
        .addDataPoints(NumberDataPoint.newBuilder().setAsDouble(12.3).setTimeUnixNano(TimeUnit.SECONDS.toNanos(epochTime)).build())
        .build();
    Metric otelMetric = Metric.newBuilder()
        .setSum(otelSum)
        .setName("test-sum")
        .build();
    wavefront.report.ReportPoint wfMetric = OtlpTestHelpers.wfReportPointGenerator()
        .setMetric("∆test-sum")
        .setTimestamp(TimeUnit.SECONDS.toMillis(epochTime))
        .setValue(12.3)
        .setHost(DEFAULT_SOURCE)
        .build();
    mockMetricsHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockMetricsHandler);

    ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder().addInstrumentationLibraryMetrics(InstrumentationLibraryMetrics.newBuilder().addMetrics(otelMetric).build()).build();
    ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockMetricsHandler);
  }

  @Test
  public void setsSourceFromResourceAttributesNotPointAttributes() {
    EasyMock.reset(mockMetricsHandler);

    Map<String, String> annotations = new HashMap<>();
    annotations.put("_source", "at-point");

    ReportPoint wfMetric = OtlpTestHelpers.wfReportPointGenerator()
        .setAnnotations(annotations)
        .setHost("at-resrc")
        .build();
    mockMetricsHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockMetricsHandler);

    NumberDataPoint otelPoint = NumberDataPoint.newBuilder()
        .addAttributes(OtlpTestHelpers.otlpAttribute("source", "at-point")).build();
    Metric otelMetric = OtlpTestHelpers.otlpGaugeGenerator(otelPoint).build();

    Resource otelResource = Resource.newBuilder().addAttributes(OtlpTestHelpers.otlpAttribute("source", "at-resrc")).build();

    ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder().setResource(otelResource)
        .addInstrumentationLibraryMetrics(InstrumentationLibraryMetrics.newBuilder().addMetrics(otelMetric).build())
        .build();
    ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockMetricsHandler);
  }
}
