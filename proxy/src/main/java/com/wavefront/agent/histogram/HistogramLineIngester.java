package com.wavefront.agent.histogram;

import com.google.common.base.Charsets;

import com.wavefront.common.TaggedMetricName;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import java.net.BindException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * A {@link ChannelInitializer} for Histogram samples via TCP.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class HistogramLineIngester extends ChannelInitializer implements Runnable {
  /**
   * Default number of seconds before the channel idle timeout handler closes the connection.
   */
  private static final int CHANNEL_IDLE_TIMEOUT_IN_SECS_DEFAULT = (int) TimeUnit.DAYS.toSeconds(1);
  private static final int MAXIMUM_OUTSTANDING_CONNECTIONS = 1024;

  private static final AtomicLong connectionId = new AtomicLong(0);

  private static final Logger logger = Logger.getLogger(HistogramLineIngester.class.getCanonicalName());
  private final Counter activeListeners = Metrics.newCounter(ExpectedAgentMetric.ACTIVE_LISTENERS.metricName);
  private final Counter bindErrors = Metrics.newCounter(ExpectedAgentMetric.LISTENERS_BIND_ERRORS.metricName);
  private final Counter connectionsAccepted;
  private final Counter connectionsIdleClosed;

  // The final handlers to be installed.
  private final ArrayList<ChannelHandler> handlers;
  private final int port;
  private int maxLength = 64 * 1024;
  private int channelIdleTimeout = CHANNEL_IDLE_TIMEOUT_IN_SECS_DEFAULT;


  public HistogramLineIngester(Collection<ChannelHandler> handlers, int port) {
    this.handlers = new ArrayList<>(handlers);
    this.port = port;
    this.connectionsAccepted = Metrics.newCounter(new TaggedMetricName("listeners", "connections.accepted",
        "port", String.valueOf(port)));
    this.connectionsIdleClosed = Metrics.newCounter(new TaggedMetricName("listeners", "connections.idle.closed",
        "port", String.valueOf(port)));
  }

  public HistogramLineIngester withMaxLength(int maxLength) {
    this.maxLength = maxLength;
    return this;
  }

  public HistogramLineIngester withChannelIdleTimeout(int channelIdleTimeout) {
    this.channelIdleTimeout = channelIdleTimeout;
    return this;
  }

  @Override
  public void run() {
    activeListeners.inc();
    ServerBootstrap bootstrap = new ServerBootstrap();

    EventLoopGroup parent;
    EventLoopGroup children;
    Class<? extends ServerChannel> socketChannelClass;
    if (Epoll.isAvailable()) {
      logger.fine("Using native socket transport for port " + port);
      parent = new EpollEventLoopGroup(1);
      children = new EpollEventLoopGroup(handlers.size());
      socketChannelClass = EpollServerSocketChannel.class;
    } else {
      logger.fine("Using NIO socket transport for port " + port);
      parent = new NioEventLoopGroup(1);
      children = new NioEventLoopGroup(handlers.size());
      socketChannelClass = NioServerSocketChannel.class;
    }

    try {
      bootstrap
          .group(parent, children)
          .channel(socketChannelClass)
          .option(ChannelOption.SO_BACKLOG, MAXIMUM_OUTSTANDING_CONNECTIONS)
          .localAddress(port)
          .childHandler(this);

      ChannelFuture f = bootstrap.bind().sync();
      f.channel().closeFuture().sync();
    } catch (final InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted");
      parent.shutdownGracefully();
      children.shutdownGracefully();
      logger.info("Listener on port " + String.valueOf(port) + " shut down");
    } catch (Exception e) {
      // ChannelFuture throws undeclared checked exceptions, so we need to handle it
      if (e instanceof BindException) {
        bindErrors.inc();
        logger.severe("Unable to start listener - port " + String.valueOf(port) + " is already in use!");
      } else {
        logger.log(Level.SEVERE, "HistogramLineIngester exception: ", e);
      }
    } finally {
      activeListeners.dec();
    }
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    // Round robin channel to handler assignment.
    int idx = (int) (Math.abs(connectionId.getAndIncrement()) % handlers.size());
    ChannelHandler handler = handlers.get(idx);
    connectionsAccepted.inc();

    // Add decoders and timeout, add handler()
    ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast(
        new LineBasedFrameDecoder(maxLength, true, false),
        new StringDecoder(Charsets.UTF_8),
        new IdleStateHandler(channelIdleTimeout, 0, 0),
        new ChannelDuplexHandler() {
          @Override
          public void userEventTriggered(ChannelHandlerContext ctx,
                                         Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
              if (((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
                connectionsIdleClosed.inc();
                logger.info("Closing idle connection to histogram client, inactivity timeout " +
                    channelIdleTimeout + "s expired: " + ctx.channel());
                ctx.close();
              }
            }
          }
        },
        handler);
  }
}
