package com.wavefront.ingester;

import com.google.common.base.Function;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Ingester thread that sets up decoders and a command handler to listen for metrics on a port.
 * @author Mike McLaughlin (mike@wavefront.com)
 */
public class TcpIngester extends Ingester {

  private static final Logger logger =
    Logger.getLogger(TcpIngester.class.getCanonicalName());

  public TcpIngester(List<Function<Channel, ChannelHandler>> decoders,
                     ChannelHandler commandHandler, int port) {
    super(decoders, commandHandler, port);
  }

  public TcpIngester(ChannelInitializer initializer, int port) {
    super(initializer, port);
  }

  public void run() {
    ServerBootstrap b = new ServerBootstrap();
    try {
      b.group(new NioEventLoopGroup(1), new NioEventLoopGroup())
        .channel(NioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG, 1024)
        .localAddress(listeningPort)
        .childHandler(initializer);

      // Start the server.
      ChannelFuture f = b.bind().sync();

      // Wait until the server socket is closed.
      f.channel().closeFuture().sync();
    } catch (final InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted", e);
    }
  }
}
