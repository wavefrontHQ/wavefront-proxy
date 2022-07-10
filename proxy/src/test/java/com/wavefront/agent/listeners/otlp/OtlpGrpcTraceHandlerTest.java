package com.wavefront.agent.listeners.otlp;

import static com.wavefront.sdk.common.Constants.*;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

import com.wavefront.agent.core.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.core.handlers.ReportableEntityHandler;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.sdk.common.WavefrontSender;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.trace.v1.Span;
import java.util.Arrays;
import java.util.HashMap;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import wavefront.report.Annotation;

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
public class OtlpGrpcTraceHandlerTest {
  private final ReportableEntityHandler<wavefront.report.Span> mockSpanHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private final ReportableEntityHandler<wavefront.report.SpanLogs> mockSpanLogsHandler =
      MockReportableEntityHandlerFactory.getMockTraceSpanLogsHandler();
  private final SpanSampler mockSampler = EasyMock.createMock(SpanSampler.class);
  private final WavefrontSender mockSender = EasyMock.createMock(WavefrontSender.class);
  private final StreamObserver<ExportTraceServiceResponse> emptyStreamObserver =
      new StreamObserver<ExportTraceServiceResponse>() {
        @Override
        public void onNext(ExportTraceServiceResponse postSpansResponse) {}

        @Override
        public void onError(Throwable throwable) {}

        @Override
        public void onCompleted() {}
      };

  @Test
  public void testMinimalSpanAndEventAndHeartbeat() throws Exception {
    // 1. Arrange
    EasyMock.reset(mockSpanHandler, mockSpanLogsHandler, mockSampler, mockSender);
    expect(mockSampler.sample(anyObject(), anyObject())).andReturn(true);
    Capture<wavefront.report.Span> actualSpan = EasyMock.newCapture();
    Capture<wavefront.report.SpanLogs> actualLogs = EasyMock.newCapture();
    mockSpanHandler.report(EasyMock.capture(actualSpan));
    mockSpanLogsHandler.report(EasyMock.capture(actualLogs));

    Capture<HashMap<String, String>> heartbeatTagsCapture = EasyMock.newCapture();
    ;
    mockSender.sendMetric(
        eq(HEART_BEAT_METRIC),
        eq(1.0),
        anyLong(),
        eq("test-source"),
        EasyMock.capture(heartbeatTagsCapture));
    expectLastCall().times(2);

    EasyMock.replay(mockSampler, mockSpanHandler, mockSpanLogsHandler, mockSender);

    Span.Event otlpEvent = OtlpTestHelpers.otlpSpanEvent(0);
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().addEvents(otlpEvent).build();
    ExportTraceServiceRequest otlpRequest = OtlpTestHelpers.otlpTraceRequest(otlpSpan);

    // 2. Act
    OtlpGrpcTraceHandler otlpGrpcTraceHandler =
        new OtlpGrpcTraceHandler(
            9876,
            mockSpanHandler,
            mockSpanLogsHandler,
            mockSender,
            null,
            mockSampler,
            () -> false,
            () -> false,
            "test-source",
            null);
    otlpGrpcTraceHandler.export(otlpRequest, emptyStreamObserver);
    otlpGrpcTraceHandler.run();
    otlpGrpcTraceHandler.close();

    // 3. Assert
    EasyMock.verify(mockSampler, mockSpanHandler, mockSpanLogsHandler, mockSender);

    wavefront.report.Span expectedSpan =
        OtlpTestHelpers.wfSpanGenerator(Arrays.asList(new Annotation("_spanLogs", "true"))).build();
    wavefront.report.SpanLogs expectedLogs =
        OtlpTestHelpers.wfSpanLogsGenerator(expectedSpan, 0).build();

    OtlpTestHelpers.assertWFSpanEquals(expectedSpan, actualSpan.getValue());
    assertEquals(expectedLogs, actualLogs.getValue());

    HashMap<String, String> actualHeartbeatTags = heartbeatTagsCapture.getValue();
    assertEquals(6, actualHeartbeatTags.size());
    assertEquals("defaultApplication", actualHeartbeatTags.get(APPLICATION_TAG_KEY));
    assertEquals("none", actualHeartbeatTags.get(CLUSTER_TAG_KEY));
    assertEquals("otlp", actualHeartbeatTags.get(COMPONENT_TAG_KEY));
    assertEquals("defaultService", actualHeartbeatTags.get(SERVICE_TAG_KEY));
    assertEquals("none", actualHeartbeatTags.get(SHARD_TAG_KEY));
    assertEquals("none", actualHeartbeatTags.get("span.kind"));
  }
}
