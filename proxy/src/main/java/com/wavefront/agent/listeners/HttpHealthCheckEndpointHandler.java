package com.wavefront.agent.listeners;

import com.wavefront.agent.auth.TokenAuthenticatorBuilder;
import com.wavefront.agent.channel.HealthCheckManager;

import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;

/**
 * A simple healthcheck-only endpoint handler. All other endpoints return a 404.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class HttpHealthCheckEndpointHandler extends AbstractHttpOnlyHandler {
  private static final Logger log = Logger.getLogger(
      HttpHealthCheckEndpointHandler.class.getCanonicalName());

  public HttpHealthCheckEndpointHandler(@Nullable final HealthCheckManager healthCheckManager,
                                        int port) {
    super(TokenAuthenticatorBuilder.create().build(), healthCheckManager, String.valueOf(port));
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) {
    StringBuilder output = new StringBuilder();
    HttpResponseStatus status = HttpResponseStatus.NOT_FOUND;
    writeHttpResponse(ctx, status, output, request);
  }
}
