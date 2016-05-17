package com.wavefront.ingester;

import com.google.common.base.Charsets;
import com.google.common.base.Function;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
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
   * Default number of seconds before the channel idle timeout handler
   * closes the connection.
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

  public Ingester(@Nullable List<Function<Channel, ChannelHandler>> decoders,
                  ChannelHandler commandHandler, int port) {
    this.listeningPort = port;
    this.createInitializer(decoders, commandHandler);
  }

  public Ingester(ChannelHandler commandHandler, int port) {
    this.listeningPort = port;
    this.createInitializer(null, commandHandler);
  }

  public Ingester(ChannelInitializer initializer, int port) {
    this.listeningPort = port;
    this.initializer = initializer;
  }

  /**
   * Creates the ChannelInitializer for this ingester
   */
  private void createInitializer(@Nullable final List<Function<Channel, ChannelHandler>> decoders, final ChannelHandler commandHandler) {
    this.initializer = new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        addDecoders(ch, decoders);
        addIdleTimeoutHandler(pipeline);
        pipeline.addLast(commandHandler);
      }
      };
  }

  /**
   * Adds an idle timeout handler to the given pipeline
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
              logger.warning("terminating connection to graphite client due to inactivity after " + CHANNEL_IDLE_TIMEOUT_IN_SECS_DEFAULT + "s: " + ctx.channel());
              ctx.close();
            }
          }
        }
      });
  }

  /**
   * Adds additional decoders passed in during construction of this object
   * (if not null).
   * @param ch the channel and pipeline to add these decoders to
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
