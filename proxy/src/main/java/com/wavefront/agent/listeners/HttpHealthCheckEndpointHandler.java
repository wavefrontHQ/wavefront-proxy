package com.wavefront.agent.listeners;

import com.wavefront.agent.auth.TokenAuthenticatorBuilder;
import com.wavefront.agent.auth.TokenValidationMethod;
import com.wavefront.agent.channel.HealthCheckManager;

import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * A simple healthcheck-only endpoint handler. All other endpoints return a 404.
 *
 * @author vasily@wavefront.com
 */
public class HttpHealthCheckEndpointHandler extends PortUnificationHandler {
  private static final Logger log = Logger.getLogger(
      HttpHealthCheckEndpointHandler.class.getCanonicalName());

  public HttpHealthCheckEndpointHandler(@Nullable final HealthCheckManager healthCheckManager,
                                        int port) {
    super(TokenAuthenticatorBuilder.create().setTokenValidationMethod(TokenValidationMethod.NONE).
        build(), healthCheckManager, String.valueOf(port), false, true);
  }

  @Override
  protected void processLine(ChannelHandlerContext ctx, String message) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) {
    StringBuilder output = new StringBuilder();
    HttpResponseStatus status = HttpResponseStatus.NOT_FOUND;
    writeHttpResponse(ctx, status, output, request);
  }
}
