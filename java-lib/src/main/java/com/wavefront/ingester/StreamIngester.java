package com.wavefront.ingester;

import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import java.net.BindException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayDecoder;

/**
 * Ingester thread that sets up decoders and a command handler to listen for metrics on a port.
 * @author Mike McLaughlin (mike@wavefront.com)
 */
@Deprecated
public class StreamIngester implements Runnable {

  protected static final Logger logger = Logger.getLogger(StreamIngester.class.getName());
  private Counter activeListeners = Metrics.newCounter(ExpectedAgentMetric.ACTIVE_LISTENERS.metricName);
  private Counter bindErrors = Metrics.newCounter(ExpectedAgentMetric.LISTENERS_BIND_ERRORS.metricName);

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
    activeListeners.inc();
    // Configure the server.
    ServerBootstrap b = new ServerBootstrap();
    EventLoopGroup parentGroup;
    EventLoopGroup childGroup;
    Class<? extends ServerChannel> socketChannelClass;
    if (Epoll.isAvailable()) {
      logger.fine("Using native socket transport for port " + listeningPort);
      parentGroup = new EpollEventLoopGroup(1);
      childGroup = new EpollEventLoopGroup();
      socketChannelClass = EpollServerSocketChannel.class;
    } else {
      logger.fine("Using NIO socket transport for port " + listeningPort);
      parentGroup = new NioEventLoopGroup(1);
      childGroup = new NioEventLoopGroup();
      socketChannelClass = NioServerSocketChannel.class;
    }
    try {
      b.group(parentGroup, childGroup)
          .channel(socketChannelClass)
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
    } catch (Exception e) {
      // ChannelFuture throws undeclared checked exceptions, so we need to handle it
      if (e instanceof BindException) {
        bindErrors.inc();
        logger.severe("Unable to start listener - port " + String.valueOf(listeningPort) + " is already in use!");
      } else {
        logger.log(Level.SEVERE, "StreamIngester exception: ", e);
      }
    } finally {
      activeListeners.dec();
    }
  }
}
