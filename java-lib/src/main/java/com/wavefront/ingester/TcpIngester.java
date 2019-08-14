package com.wavefront.ingester;

import com.google.common.base.Function;

import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import java.net.BindException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Ingester thread that sets up decoders and a command handler to listen for metrics on a port.
 * @author Mike McLaughlin (mike@wavefront.com)
 */
public class TcpIngester extends Ingester {

  private static final Logger logger =
    Logger.getLogger(TcpIngester.class.getCanonicalName());
  private Counter activeListeners = Metrics.newCounter(ExpectedAgentMetric.ACTIVE_LISTENERS.metricName);
  private Counter bindErrors = Metrics.newCounter(ExpectedAgentMetric.LISTENERS_BIND_ERRORS.metricName);

  @Deprecated
  public TcpIngester(List<Function<Channel, ChannelHandler>> decoders,
                     ChannelHandler commandHandler, int port) {
    super(decoders, commandHandler, port);
  }

  public TcpIngester(ChannelInitializer initializer, int port) {
    super(initializer, port);
  }

  public void run() {
    activeListeners.inc();
    ServerBootstrap b = new ServerBootstrap();
    EventLoopGroup parentGroup;
    EventLoopGroup childGroup;
    Class<? extends ServerChannel> socketChannelClass;
    if (Epoll.isAvailable()) {
      logger.fine("Using native socket transport for port " + listeningPort);
      parentGroup = new EpollEventLoopGroup(1);
      childGroup = new EpollEventLoopGroup();
      socketChannelClass = EpollServerSocketChannel.class;
    } else {
      logger.fine("Using NIO socket transport for port " + listeningPort);
      parentGroup = new NioEventLoopGroup(1);
      childGroup = new NioEventLoopGroup();
      socketChannelClass = NioServerSocketChannel.class;
    }
    try {
      b.group(parentGroup, childGroup)
        .channel(socketChannelClass)
        .option(ChannelOption.SO_BACKLOG, 1024)
        .localAddress(listeningPort)
        .childHandler(initializer);

      if (parentChannelOptions != null) {
        for (Map.Entry<ChannelOption<?>, ?> entry : parentChannelOptions.entrySet())
        {
          b.option((ChannelOption<Object>) entry.getKey(), entry.getValue());
        }
      }
      if (childChannelOptions != null) {
        for (Map.Entry<ChannelOption<?>, ?> entry : childChannelOptions.entrySet())
        {
          b.childOption((ChannelOption<Object>) entry.getKey(), entry.getValue());
        }
      }

      // Start the server.
      ChannelFuture f = b.bind().sync();

      // Wait until the server socket is closed.
      f.channel().closeFuture().sync();
    } catch (final InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted");
      parentGroup.shutdownGracefully();
      childGroup.shutdownGracefully();
      logger.info("Listener on port " + listeningPort + " shut down");
    } catch (Exception e) {
      // ChannelFuture throws undeclared checked exceptions, so we need to handle it
      //noinspection ConstantConditions
      if (e instanceof BindException) {
        bindErrors.inc();
        logger.severe("Unable to start listener - port " + listeningPort + " is already in use!");
      } else {
        logger.log(Level.SEVERE, "TcpIngester exception: ", e);
      }
    } finally {
      activeListeners.dec();
    }
  }
}
