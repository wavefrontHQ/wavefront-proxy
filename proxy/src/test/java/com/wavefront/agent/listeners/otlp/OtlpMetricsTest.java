package com.wavefront.agent.listeners.otlp;

import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import wavefront.report.ReportPoint;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
  private OtlpGrpcMetricsHandler subject;
  private ReportableEntityHandler<ReportPoint, String> mockMetricsHandler = MockReportableEntityHandlerFactory.getMockReportPointHandler();

  @Before
  public void setup() {
    Supplier<ReportableEntityPreprocessor> preprocessorSupplier = ReportableEntityPreprocessor::new;
    subject = new OtlpGrpcMetricsHandler(mockMetricsHandler, preprocessorSupplier);
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
        .build();
    mockMetricsHandler.report(wfMetric);
    EasyMock.expectLastCall();

    EasyMock.replay(mockMetricsHandler);

    ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder().addInstrumentationLibraryMetrics(InstrumentationLibraryMetrics.newBuilder().addMetrics(otelMetric).build()).build();
    ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    subject.export(request, emptyStreamObserver);

    EasyMock.verify(mockMetricsHandler);
  }
}