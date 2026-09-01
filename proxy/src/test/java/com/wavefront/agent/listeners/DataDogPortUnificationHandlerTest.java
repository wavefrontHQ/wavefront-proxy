package com.wavefront.agent.listeners;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertThrows;

import com.wavefront.agent.channel.NoopHealthCheckManager;
import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.net.URISyntaxException;
import org.apache.http.client.HttpClient;
import org.junit.Test;
import wavefront.report.ReportPoint;

public class DataDogPortUnificationHandlerTest {

  /**
   * A request-target that doesn't start with '/' (not enforceable by Netty on the wire) must
   * never be relayed - otherwise concatenating it onto the relay target's authority lets an
   * attacker redirect the relay to an arbitrary host (SSRF via userinfo injection).
   */
  @Test
  public void testRelayRejectsRequestTargetNotStartingWithSlash() {
    HttpClient mockRelayClient = createMock(HttpClient.class);
    replay(mockRelayClient); // no calls expected - the relay must never fire

    ReportableEntityHandler<ReportPoint, String> mockPointHandler =
        MockReportableEntityHandlerFactory.getMockReportPointHandler();

    DataDogPortUnificationHandler handler =
        new DataDogPortUnificationHandler(
            "50000",
            new NoopHealthCheckManager(),
            mockPointHandler,
            1,
            true,
            false,
            false,
            mockRelayClient,
            "https://api.datadoghq.com",
            null);

    FullHttpRequest httpRequest =
        new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.POST,
            "@evil.com/api/v1/series/",
            Unpooled.EMPTY_BUFFER);

    ChannelHandlerContext mockCtx = createNiceMock(ChannelHandlerContext.class);
    replay(mockCtx);

    assertThrows(
        URISyntaxException.class, () -> handler.handleHttpMessage(mockCtx, httpRequest));

    verify(mockRelayClient);
  }
}
