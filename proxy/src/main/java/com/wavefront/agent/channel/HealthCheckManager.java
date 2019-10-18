package com.wavefront.agent.channel;

import javax.annotation.Nonnull;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;

/**
 * Centrally manages healthcheck statuses (for controlling load balancers).
 *
 * @author vasily@wavefront.com
 */
public interface HealthCheckManager {
  HttpResponse getHealthCheckResponse(ChannelHandlerContext ctx, @Nonnull FullHttpRequest request);

  boolean isHealthy(int port);

  void setHealthy(int port);

  void setUnhealthy(int port);

  void setAllHealthy();

  void setAllUnhealthy();

  void enableHealthcheck(int port);
}


