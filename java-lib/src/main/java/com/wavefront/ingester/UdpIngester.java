package com.wavefront.ingester;

import com.google.common.base.Function;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
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

  public UdpIngester(List<Function<Channel, ChannelHandler>> decoders,
                     ChannelHandler commandHandler, int port) {
    super(decoders, commandHandler, port);
  }

  @Override
  public void run() {
    Bootstrap bootstrap = new Bootstrap();
    EventLoopGroup group = new NioEventLoopGroup();
    try {
      bootstrap
          .group(group)
          .channel(NioDatagramChannel.class)
          .localAddress(listeningPort)
          .handler(initializer);

      // Start the server.
      bootstrap.bind().sync().channel().closeFuture().sync();
    } catch (final InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted", e);
    } finally {
      group.shutdownGracefully();
    }
  }
}
