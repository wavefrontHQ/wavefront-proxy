package com.wavefront.agent.listeners;

import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;

import com.wavefront.agent.auth.TokenAuthenticatorBuilder;
import com.wavefront.agent.channel.HealthCheckManager;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import javax.annotation.Nullable;

/** A simple healthcheck-only endpoint handler. All other endpoints return a 404. */
@ChannelHandler.Sharable
public class HttpHealthCheckEndpointHandler extends AbstractHttpOnlyHandler {

  public HttpHealthCheckEndpointHandler(
      @Nullable final HealthCheckManager healthCheckManager, int port) {
    super(TokenAuthenticatorBuilder.create().build(), healthCheckManager, port);
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx, final FullHttpRequest request) {
    StringBuilder output = new StringBuilder();
    HttpResponseStatus status = HttpResponseStatus.NOT_FOUND;
    writeHttpResponse(ctx, status, output, request);
  }
}
