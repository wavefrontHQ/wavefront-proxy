package com.wavefront.ingester.graphite;

import com.google.common.base.Charsets;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class GraphiteIngester implements Runnable {

  private static final Logger logger = Logger.getLogger(GraphiteIngester.class.getCanonicalName());
  private static final int CHANNEL_IDLE_TIMEOUT_IN_SECS = (int) TimeUnit.DAYS.toSeconds(1);

  private final ChannelHandler commandHandler;
  private boolean decodeGraphite;
  private boolean implicitHosts;
  private final int listeningPort;

  public GraphiteIngester(ChannelHandler commandHandler, int port, boolean decodeGraphite) {
    this(commandHandler, port, decodeGraphite, true);
  }

  public GraphiteIngester(ChannelHandler commandHandler, int port,
                          boolean decodeGraphite, boolean implicitHosts) {
    this.listeningPort = port;
    this.commandHandler = commandHandler;
    this.decodeGraphite = decodeGraphite;
    this.implicitHosts = implicitHosts;
  }

  public void run() {

    // Configure the server.
    ServerBootstrap b = new ServerBootstrap();
    try {
      b.group(new NioEventLoopGroup(), new NioEventLoopGroup())
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, 100)
          .localAddress(listeningPort)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
              ChannelPipeline pipeline = ch.pipeline();
              pipeline.addLast(new LineBasedFrameDecoder(4096, true, true));
              pipeline.addLast(new StringDecoder(Charsets.UTF_8));
              if (implicitHosts) {
                if (decodeGraphite) {
                  // Full decoding to a report point
                  pipeline.addLast(new GraphiteDecoder(ch.remoteAddress().getHostName()));
                } else {
                  // Implicit host annotation only; left in string format
                  pipeline.addLast(new GraphiteHostAnnotator(ch.remoteAddress().getHostName()));
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
