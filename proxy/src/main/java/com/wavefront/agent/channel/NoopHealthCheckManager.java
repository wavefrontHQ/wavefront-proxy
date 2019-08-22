package com.wavefront.agent.channel;

import javax.annotation.Nonnull;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;

/**
 * A no-op health check manager.
 *
 * @author vasily@wavefront.com.
 */
public class NoopHealthCheckManager implements HealthCheckManager {
  @Override
  public HttpResponse getHealthCheckResponse(ChannelHandlerContext ctx,
                                             @Nonnull FullHttpRequest request) {
    return null;
  }

  @Override
  public boolean isHealthy(int port) {
    return true;
  }

  @Override
  public void setHealthy(int port) {
  }

  @Override
  public void setUnhealthy(int port) {
  }

  @Override
  public void setAllHealthy() {
  }

  @Override
  public void setAllUnhealthy() {
  }

  @Override
  public void enableHealthcheck(int port) {
  }
}
