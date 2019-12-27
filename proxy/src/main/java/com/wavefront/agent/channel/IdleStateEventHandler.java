package com.wavefront.agent.channel;

import com.yammer.metrics.core.Counter;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

/**
 * Disconnect idle clients (handle READER_IDLE events triggered by IdleStateHandler)
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class IdleStateEventHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = Logger.getLogger(
      IdleStateEventHandler.class.getCanonicalName());

  private final Counter idleClosedConnections;

  public IdleStateEventHandler(@Nonnull Counter idleClosedConnectionsCounter) {
    this.idleClosedConnections = idleClosedConnectionsCounter;
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
    if (evt instanceof IdleStateEvent) {
      if (((IdleStateEvent) evt).state() == IdleState.READER_IDLE) { // close idle connections
        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        InetSocketAddress localAddress = (InetSocketAddress) ctx.channel().localAddress();
        logger.info("Closing idle connection on port " + localAddress.getPort() +
            ", remote address: " + remoteAddress.getAddress().getHostAddress());
        idleClosedConnections.inc();
        ctx.channel().close();
      }
    }
  }
}
