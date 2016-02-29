package com.wavefront.ingester;

import com.google.common.base.Charsets;
import com.google.common.base.Function;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.InetSocketAddress;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.Channel;

/**
 * Ingester thread that sets up decoders and a command handler to listen for metrics on a port.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class UdpIngester extends Ingester {

  private static final Logger logger =
    Logger.getLogger(UdpIngester.class.getCanonicalName());

  public UdpIngester(List<Function<Channel, ChannelHandler>> decoders,
                     ChannelHandler commandHandler, int port) {
    super(decoders, commandHandler, port);
  }

  public UdpIngester(ChannelHandler commandHandler, int port) {
      super(commandHandler, port);
  }

  public void run() {
    // Configure the server.
    final NioEventLoopGroup group = new NioEventLoopGroup();
    try {
      final Bootstrap b = new Bootstrap();
      b.group(group)
        .channel(NioDatagramChannel.class)
        .option(ChannelOption.SO_BROADCAST, true)
        .handler(commandHandler);

      // Start the server.
      b.bind(listeningPort).sync().channel().closeFuture().await();
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted", e);

      // Server was interrupted
      e.printStackTrace();
    } finally {
      group.shutdownGracefully();
    }
  }
}
