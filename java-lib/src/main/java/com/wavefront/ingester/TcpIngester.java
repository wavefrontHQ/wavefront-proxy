package com.wavefront.ingester;

import com.google.common.base.Function;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

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
    NioEventLoopGroup parentGroup = new NioEventLoopGroup(1);
    NioEventLoopGroup childGroup = new NioEventLoopGroup();
    try {
      b.group(parentGroup, childGroup)
        .channel(NioServerSocketChannel.class)
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
      logger.info("Listener on port " + String.valueOf(listeningPort) + " shut down");
    }
  }
}
