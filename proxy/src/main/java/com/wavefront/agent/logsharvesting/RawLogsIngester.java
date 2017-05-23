package com.wavefront.agent.logsharvesting;

import com.wavefront.agent.histogram.HistogramLineIngester;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.function.Supplier;
import java.util.logging.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.CharsetUtil;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class RawLogsIngester {

  private static final Logger logger = Logger.getLogger(RawLogsIngester.class.getCanonicalName());
  private LogsIngester logsIngester;
  private int port;
  private Supplier<Long> now;
  private Counter received;

  public RawLogsIngester(LogsIngester logsIngester, int port, Supplier<Long> now) {
    this.logsIngester = logsIngester;
    this.port = port;
    this.now = now;
    this.received = Metrics.newCounter(new MetricName("logsharvesting", "", "raw-received"));
  }

  public void listen() throws InterruptedException {
    ServerBootstrap serverBootstrap = new ServerBootstrap();
    EventLoopGroup acceptorGroup;
    EventLoopGroup handlerGroup;
    Class<? extends ServerChannel> socketChannelClass;
    if (Epoll.isAvailable()) {
      logger.fine("Using native socket transport for port " + port);
      acceptorGroup = new EpollEventLoopGroup(2);
      handlerGroup = new EpollEventLoopGroup(10);
      socketChannelClass = EpollServerSocketChannel.class;
    } else {
      logger.fine("Using NIO socket transport for port " + port);
      acceptorGroup = new NioEventLoopGroup(2);
      handlerGroup = new NioEventLoopGroup(10);
      socketChannelClass = NioServerSocketChannel.class;
    }

    serverBootstrap.group(acceptorGroup, handlerGroup)
        .channel(socketChannelClass)
        .childHandler(new SocketInitializer())
        .option(ChannelOption.SO_BACKLOG, 5)
        .option(ChannelOption.SO_KEEPALIVE, true);

    serverBootstrap.bind(port).sync();
  }

  public void ingestLog(ChannelHandlerContext ctx, String log) {
    logsIngester.ingestLog(new LogsMessage() {
      @Override
      public String getLogLine() {
        return log;
      }

      @Override
      public String hostOrDefault(String fallbackHost) {
        if (!(ctx.channel().remoteAddress() instanceof InetSocketAddress)) return fallbackHost;
        InetSocketAddress inetSocketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        InetAddress inetAddress = inetSocketAddress.getAddress();
        String host = inetAddress.getCanonicalHostName();
        if (host == null || host.equals("")) return fallbackHost;
        return host;
      }
    });
  }

  private class SocketInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      ChannelPipeline channelPipeline = ch.pipeline();
      channelPipeline.addLast(LineBasedFrameDecoder.class.getName(), new LineBasedFrameDecoder(4096));
      channelPipeline.addLast(StringDecoder.class.getName(), new StringDecoder(CharsetUtil.UTF_8));
      channelPipeline.addLast("logsIngestionHandler", new SimpleChannelInboundHandler<String>() {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
          received.inc();
          ingestLog(ctx, msg);
        }
      });
    }
  }

}
