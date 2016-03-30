package com.wavefront.ingester;

import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.bytes.ByteArrayDecoder;

/**
 * Ingester thread that sets up decoders and a command handler to listen for metrics on a port.
 */
public class StreamIngester implements Runnable {

  public interface FrameDecoderFactory {
    ChannelInboundHandler getDecoder();
  }

  private static final Logger logger = Logger.getLogger(Ingester.class.getCanonicalName());

  private final ChannelHandler commandHandler;
  private final int listeningPort;
  private final FrameDecoderFactory frameDecoderFactory;

  public StreamIngester(FrameDecoderFactory frameDecoderFactory,
                        ChannelHandler commandHandler, int port) {
    this.listeningPort = port;
    this.commandHandler = commandHandler;
    this.frameDecoderFactory = frameDecoderFactory;
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
              pipeline.addLast("frame decoder", frameDecoderFactory.getDecoder());
              pipeline.addLast("To Byte Array", new ByteArrayDecoder());
              pipeline.addLast(commandHandler);
            }
          });

      // Start the server.
      ChannelFuture f = b.bind().sync();

      // Wait until the server socket is closed.
      f.channel().closeFuture().sync();
    } catch (final InterruptedException e) {
      // Server was interrupted
      e.printStackTrace();
    }
  }
}
