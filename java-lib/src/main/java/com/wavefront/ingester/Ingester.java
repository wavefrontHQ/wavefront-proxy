package com.wavefront.ingester;

import com.google.common.base.Charsets;
import com.google.common.base.Function;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
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
public class Ingester implements Runnable {

  private static final Logger logger = Logger.getLogger(Ingester.class.getCanonicalName());

  private static final int CHANNEL_IDLE_TIMEOUT_IN_SECS = (int) TimeUnit.DAYS.toSeconds(1);

  @Nullable
  private final List<Function<SocketChannel, ChannelHandler>> decoders;
  private final ChannelHandler commandHandler;
  private final int listeningPort;

  public Ingester(List<Function<SocketChannel, ChannelHandler>> decoders,
                  ChannelHandler commandHandler, int port) {
    this.listeningPort = port;
    this.commandHandler = commandHandler;
    this.decoders = decoders;
  }

  public Ingester(ChannelHandler commandHandler, int port) {
    this.listeningPort = port;
    this.commandHandler = commandHandler;
    this.decoders = null;
  }

  public void run() {
    // Configure the server.
    ServerBootstrap b = new ServerBootstrap();
    try {
      b.group(new NioEventLoopGroup(1), new NioEventLoopGroup())
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, 1024)
          .localAddress(listeningPort)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
              ChannelPipeline pipeline = ch.pipeline();
              pipeline.addLast(new LineBasedFrameDecoder(4096, true, true));
              pipeline.addLast(new StringDecoder(Charsets.UTF_8));
              if (decoders != null) {
                for (Function<SocketChannel, ChannelHandler> handler : decoders) {
                  pipeline.addLast(handler.apply(ch));
                }
              }
              // Shared across all reports for proper batching
              pipeline.addLast("idleStateHandler", new IdleStateHandler(CHANNEL_IDLE_TIMEOUT_IN_SECS,
                  0, 0));
              pipeline.addLast("idleChannelTerminator", new ChannelDuplexHandler() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                  if (evt instanceof IdleStateEvent) {
                    if (((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
                      logger.warning("terminating connection to graphite client due to inactivity after " +
                          CHANNEL_IDLE_TIMEOUT_IN_SECS + "s: " + ctx.channel());
                      ctx.close();
                    }
                  }
                }
              });
              pipeline.addLast(commandHandler);
            }
          });

      // Start the server.
      ChannelFuture f = b.bind().sync();

      // Wait until the server socket is closed.
      f.channel().closeFuture().sync();
    } catch (InterruptedException e) {
      // Server was interrupted
      e.printStackTrace();
    }
  }
}
