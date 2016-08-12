package com.wavefront.ingester;

import java.util.Map;
import java.util.logging.Logger;
import java.util.logging.Level;

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
 * @author Mike McLaughlin (mike@wavefront.com)
 */
public class StreamIngester implements Runnable {

  protected static final Logger logger = Logger.getLogger(StreamIngester.class.getName());

  public interface FrameDecoderFactory {
    ChannelInboundHandler getDecoder();
  }

  private final ChannelHandler commandHandler;
  private final int listeningPort;
  private final FrameDecoderFactory frameDecoderFactory;

  @Nullable
  protected Map<ChannelOption<?>, ?> parentChannelOptions;
  @Nullable
  protected Map<ChannelOption<?>, ?> childChannelOptions;

  public StreamIngester(FrameDecoderFactory frameDecoderFactory,
                        ChannelHandler commandHandler, int port) {
    this.listeningPort = port;
    this.commandHandler = commandHandler;
    this.frameDecoderFactory = frameDecoderFactory;
  }

  public StreamIngester withParentChannelOptions(Map<ChannelOption<?>, ?> parentChannelOptions) {
    this.parentChannelOptions = parentChannelOptions;
    return this;
  }

  public StreamIngester withChildChannelOptions(Map<ChannelOption<?>, ?> childChannelOptions) {
    this.childChannelOptions = childChannelOptions;
    return this;
  }


  public void run() {
    // Configure the server.
    ServerBootstrap b = new ServerBootstrap();
    NioEventLoopGroup parentGroup = new NioEventLoopGroup(1);
    NioEventLoopGroup childGroup = new NioEventLoopGroup();
    try {
      b.group(parentGroup, childGroup)
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, 1024)
          .localAddress(listeningPort)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
              ChannelPipeline pipeline = ch.pipeline();
              pipeline.addLast("frame decoder", frameDecoderFactory.getDecoder());
              pipeline.addLast("byte array decoder", new ByteArrayDecoder());
              pipeline.addLast(commandHandler);
            }
          });

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
