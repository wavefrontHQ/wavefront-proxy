package com.wavefront.agent.listeners.otlp;

import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.sdk.common.WavefrontSender;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.COMPONENT_TAG_KEY;
import static com.wavefront.sdk.common.Constants.HEART_BEAT_METRIC;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link OtlpHttpHandler}.
 *
 * @author Glenn Oppegard (goppegard@vmware.com)
 */

public class OtlpHttpHandlerTest {
  private final ReportableEntityHandler<Span, String> mockTraceHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private final ReportableEntityHandler<SpanLogs, String> mockSpanLogsHandler =
      MockReportableEntityHandlerFactory.getMockTraceSpanLogsHandler();
  private final SpanSampler mockSampler = EasyMock.createMock(SpanSampler.class);
  private final WavefrontSender mockSender = EasyMock.createMock(WavefrontSender.class);
  private final ReportableEntityHandlerFactory mockHandlerFactory =
      MockReportableEntityHandlerFactory.createMockHandlerFactory(null, null, null,
          mockTraceHandler, mockSpanLogsHandler, null);
  private final ChannelHandlerContext mockCtx = EasyMock.createNiceMock(ChannelHandlerContext.class);

  @Before
  public void setup() {
    EasyMock.reset(mockTraceHandler, mockSpanLogsHandler, mockSampler, mockSender, mockCtx);
  }

  @Test
  public void testHeartbeatEmitted() throws Exception {
    EasyMock.expect(mockSampler.sample(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(true);
    Capture<HashMap<String, String>> heartbeatTagsCapture = EasyMock.newCapture();
    mockSender.sendMetric(eq(HEART_BEAT_METRIC), eq(1.0), anyLong(),
        eq("defaultSource"), EasyMock.capture(heartbeatTagsCapture));
    expectLastCall().times(2);
    EasyMock.replay(mockSampler, mockSender, mockCtx);

    OtlpHttpHandler handler = new OtlpHttpHandler(mockHandlerFactory, null, null, "4318",
        mockSender, null, mockSampler, "defaultSource", null);
    io.opentelemetry.proto.trace.v1.Span otlpSpan =
        OtlpTestHelpers.otlpSpanGenerator().build();
    ExportTraceServiceRequest otlpRequest = OtlpTestHelpers.otlpTraceRequest(otlpSpan);
    ByteBuf body = Unpooled.copiedBuffer(otlpRequest.toByteArray());
    HttpHeaders headers = new DefaultHttpHeaders().add("content-type", "application/x-protobuf");
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
        "http://localhost:4318/v1/traces", body, headers, EmptyHttpHeaders.INSTANCE);

    handler.handleHttpMessage(mockCtx, request);
    handler.run();

    EasyMock.verify(mockSampler, mockSender);
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
