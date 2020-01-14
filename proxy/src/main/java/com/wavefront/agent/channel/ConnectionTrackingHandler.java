package com.wavefront.agent.channel;

import com.yammer.metrics.core.Counter;

import javax.annotation.Nonnull;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Track the number of currently active connections and total count of accepted incoming connections.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class ConnectionTrackingHandler extends ChannelInboundHandlerAdapter {

  private final Counter acceptedConnections;
  private final Counter activeConnections;

  public ConnectionTrackingHandler(@Nonnull Counter acceptedConnectionsCounter,
                                   @Nonnull Counter activeConnectionsCounter) {
    this.acceptedConnections = acceptedConnectionsCounter;
    this.activeConnections = activeConnectionsCounter;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    activeConnections.inc();
    acceptedConnections.inc();
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    activeConnections.dec();
    super.channelInactive(ctx);
  }
}
