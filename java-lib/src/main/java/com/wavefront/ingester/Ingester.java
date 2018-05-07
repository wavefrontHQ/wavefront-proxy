package com.wavefront.ingester;

import com.google.common.base.Function;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Ingester thread that sets up decoders and a command handler to listen for metrics on a port.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public abstract class Ingester implements Runnable {
  private static final Logger logger = Logger.getLogger(Ingester.class.getCanonicalName());

  /**
   * Default number of seconds before the channel idle timeout handler closes the connection.
   */
  private static final int CHANNEL_IDLE_TIMEOUT_IN_SECS_DEFAULT = (int) TimeUnit.DAYS.toSeconds(1);

  /**
   * The port that this ingester should be listening on
   */
  protected final int listeningPort;

  /**
   * The channel initializer object for the netty channel
   */
  protected ChannelInitializer initializer;

  /**
   * Counter metrics for accepted and terminated connections
   */
  private Counter connectionsAccepted;
  private Counter connectionsIdleClosed;

  @Nullable
  protected Map<ChannelOption<?>, ?> parentChannelOptions;
  @Nullable
  protected Map<ChannelOption<?>, ?> childChannelOptions;

  public Ingester(@Nullable List<Function<Channel, ChannelHandler>> decoders,
                  ChannelHandler commandHandler, int port) {
    this.listeningPort = port;
    this.createInitializer(decoders, commandHandler);
    initMetrics(port);
  }

  public Ingester(ChannelHandler commandHandler, int port) {
    this.listeningPort = port;
    this.createInitializer(null, commandHandler);
    initMetrics(port);
  }

  public Ingester(ChannelInitializer initializer, int port) {
    this.listeningPort = port;
    this.initializer = initializer;
    initMetrics(port);
  }

  public Ingester withParentChannelOptions(Map<ChannelOption<?>, ?> parentChannelOptions) {
    this.parentChannelOptions = parentChannelOptions;
    return this;
  }

  public Ingester withChildChannelOptions(Map<ChannelOption<?>, ?> childChannelOptions) {
    this.childChannelOptions = childChannelOptions;
    return this;
  }

  private void initMetrics(int port) {
    this.connectionsAccepted = Metrics.newCounter(new TaggedMetricName("listeners", "connections.accepted",
        "port", String.valueOf(port)));
    this.connectionsIdleClosed = Metrics.newCounter(new TaggedMetricName("listeners", "connections.idle.closed",
        "port", String.valueOf(port)));
  }

  /**
   * Creates the ChannelInitializer for this ingester
   */
  private void createInitializer(@Nullable final List<Function<Channel, ChannelHandler>> decoders, final ChannelHandler commandHandler) {
    this.initializer = new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        connectionsAccepted.inc();
        ChannelPipeline pipeline = ch.pipeline();
        addDecoders(ch, decoders);
        addIdleTimeoutHandler(pipeline);
        pipeline.addLast(commandHandler);
      }
    };
  }

  /**
   * Adds an idle timeout handler to the given pipeline
   *
   * @param pipeline the pipeline to add the idle timeout handler
   */
  protected void addIdleTimeoutHandler(final ChannelPipeline pipeline) {
    // Shared across all reports for proper batching
    pipeline.addLast("idleStateHandler",
        new IdleStateHandler(CHANNEL_IDLE_TIMEOUT_IN_SECS_DEFAULT,
            0, 0));
    pipeline.addLast("idleChannelTerminator", new ChannelDuplexHandler() {
      @Override
      public void userEventTriggered(ChannelHandlerContext ctx,
                                     Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
          if (((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
            connectionsIdleClosed.inc();
            logger.warning("Closing idle connection, client inactivity timeout expired: " + ctx.channel());
            ctx.close();
          }
        }
      }
    });
  }

  /**
   * Adds additional decoders passed in during construction of this object (if not null).
   *
   * @param ch       the channel and pipeline to add these decoders to
   * @param decoders the list of decoders to add to the channel
   */
  protected void addDecoders(final Channel ch, @Nullable List<Function<Channel, ChannelHandler>> decoders) {
    if (decoders != null) {
      ChannelPipeline pipeline = ch.pipeline();
      for (Function<Channel, ChannelHandler> handler : decoders) {
        pipeline.addLast(handler.apply(ch));
      }
    }
  }

}
