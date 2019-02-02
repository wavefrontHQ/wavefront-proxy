package com.wavefront.agent.listeners.tracing;

import com.google.common.collect.ImmutableList;

import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.sdk.entities.tracing.sampling.RateSampler;

import org.easymock.EasyMock;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import wavefront.report.Annotation;
import wavefront.report.Span;
import zipkin2.Endpoint;
import zipkin2.codec.SpanBytesEncoder;

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

public class ZipkinPortUnificationHandlerTest {
  private ReportableEntityHandler<Span> mockTraceHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private long startTime = System.currentTimeMillis();

  @Test
  public void testZipkinHandler() {
    ZipkinPortUnificationHandler handler = new ZipkinPortUnificationHandler("9411", mockTraceHandler, null,
        new AtomicBoolean(false), null, new RateSampler(1.0D), false);

    Endpoint localEndpoint1 = Endpoint.newBuilder().serviceName("frontend").ip("10.0.0.1").build();
    zipkin2.Span spanServer1 = zipkin2.Span.newBuilder().
        traceId("2822889fe47043bd").
        id("2822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getservice").
        timestamp(startTime * 1000).
        duration(1234 * 1000).
        localEndpoint(localEndpoint1).
        putTag("http.method", "GET").
        putTag("http.url", "none+h1c://localhost:8881/").
        putTag("http.status_code", "200").
        build();

    Endpoint localEndpoint2 = Endpoint.newBuilder().serviceName("backend").ip("10.0.0.1").build();
    zipkin2.Span spanServer2 = zipkin2.Span.newBuilder().
        traceId("2822889fe47043bd").
        id("d6ab73f8a3930ae8").
        parentId("2822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getbackendservice").
        timestamp(startTime * 1000).
        duration(2234 * 1000).
        localEndpoint(localEndpoint2).
        putTag("http.method", "GET").
        putTag("http.url", "none+h2c://localhost:9000/api").
        putTag("http.status_code", "200").
        build();

    List<zipkin2.Span> zipkinSpanList = ImmutableList.of(spanServer1, spanServer2);

    // Validate all codecs i.e. JSON_V1, JSON_V2, THRIFT and PROTO3.
    for (SpanBytesEncoder encoder : SpanBytesEncoder.values()) {
      ByteBuf content = Unpooled.copiedBuffer(encoder.encodeList(zipkinSpanList));
      // take care of mocks.
      doMockLifecycle(mockTraceHandler);
      ChannelHandlerContext mockCtx = createNiceMock(ChannelHandlerContext.class);
      doMockLifecycle(mockCtx);
      FullHttpRequest httpRequest = new DefaultFullHttpRequest(
          HttpVersion.HTTP_1_1,
          HttpMethod.POST,
          "http://localhost:9411/api/v1/spans",
          content,
          true
      );
      handler.handleHttpMessage(mockCtx, httpRequest);
      verify(mockTraceHandler);
    }
  }

  private void doMockLifecycle(ChannelHandlerContext mockCtx) {
    reset(mockCtx);
    EasyMock.expect(mockCtx.write(EasyMock.isA(FullHttpResponse.class))).andReturn(null);
    EasyMock.replay(mockCtx);
  }

  private void doMockLifecycle(ReportableEntityHandler<Span> mockTraceHandler) {
    // Reset mock
    reset(mockTraceHandler);

    // Set Expectation
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime).
        setDuration(1234).
        setName("getservice").
        setSource("10.0.0.1").
        setSpanId("00000000-0000-0000-2822-889fe47043bd").
        setTraceId("00000000-0000-0000-2822-889fe47043bd").
        // Note: Order of annotations list matters for this unit test.
        setAnnotations(ImmutableList.of(
            new Annotation("span.kind", "server"),
            new Annotation("service", "frontend"),
            new Annotation("http.method", "GET"),
            new Annotation("http.status_code", "200"),
            new Annotation("http.url", "none+h1c://localhost:8881/"),
            new Annotation("application", "Zipkin"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"))).
        build());
    expectLastCall();

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime).
        setDuration(2234).
        setName("getbackendservice").
        setSource("10.0.0.1").
        setSpanId("00000000-0000-0000-d6ab-73f8a3930ae8").
        setTraceId("00000000-0000-0000-2822-889fe47043bd").
        // Note: Order of annotations list matters for this unit test.
        setAnnotations(ImmutableList.of(
            new Annotation("parent", "00000000-0000-0000-2822-889fe47043bd"),
            new Annotation("span.kind", "server"),
            new Annotation("service", "backend"),
            new Annotation("http.method", "GET"),
            new Annotation("http.status_code", "200"),
            new Annotation("http.url", "none+h2c://localhost:9000/api"),
            new Annotation("application", "Zipkin"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"))).
        build());
    expectLastCall();

    // Replay
    replay(mockTraceHandler);
  }
}
