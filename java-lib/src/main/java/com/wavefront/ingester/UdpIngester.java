package com.wavefront.ingester;

import com.google.common.base.Function;

import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import java.net.BindException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;

/**
 * Bootstrapping for datagram ingester channels on a socket.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class UdpIngester extends Ingester {
  private static final Logger logger =
      Logger.getLogger(UdpIngester.class.getCanonicalName());
  private Counter activeListeners = Metrics.newCounter(ExpectedAgentMetric.ACTIVE_LISTENERS.metricName);

  public UdpIngester(List<Function<Channel, ChannelHandler>> decoders,
                     ChannelHandler commandHandler, int port) {
    super(decoders, commandHandler, port);
  }

  @Override
  public void run() {
    activeListeners.inc();
    Bootstrap bootstrap = new Bootstrap();
    EventLoopGroup group;
    Class<? extends Channel> datagramChannelClass;
    if (Epoll.isAvailable()) {
      logger.fine("Using native socket transport for port " + listeningPort);
      group = new EpollEventLoopGroup();
      datagramChannelClass = EpollDatagramChannel.class;
    } else {
      logger.fine("Using NIO socket transport for port " + listeningPort);
      group = new NioEventLoopGroup();
      datagramChannelClass = NioDatagramChannel.class;
    }
    try {
      bootstrap
          .group(group)
          .channel(datagramChannelClass)
          .localAddress(listeningPort)
          .handler(initializer);

      // Start the server.
      bootstrap.bind().sync().channel().closeFuture().sync();
    } catch (final InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted", e);
    } catch (Exception e) {
      // ChannelFuture throws undeclared checked exceptions, so we need to handle it
      if (e instanceof BindException) {
        logger.severe("Unable to start listener - port " + String.valueOf(listeningPort) + " is already in use!");
      } else {
        logger.log(Level.SEVERE, "UdpIngester exception: ", e);
      }
    } finally {
      activeListeners.dec();
      group.shutdownGracefully();
    }
  }
}
