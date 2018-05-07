package com.wavefront.agent.logsharvesting;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelDuplexHandler;
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
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
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
  private final Counter connectionsAccepted;
  private final Counter connectionsIdleClosed;
  private int maxLength = 4096;
  private int channelIdleTimeout = (int) TimeUnit.HOURS.toSeconds(1);

  public RawLogsIngester(LogsIngester logsIngester, int port, Supplier<Long> now) {
    this.logsIngester = logsIngester;
    this.port = port;
    this.now = now;
    this.received = Metrics.newCounter(new MetricName("logsharvesting", "", "raw-received"));
    this.connectionsAccepted = Metrics.newCounter(new TaggedMetricName("listeners", "connections.accepted",
        "port", String.valueOf(port)));
    this.connectionsIdleClosed = Metrics.newCounter(new TaggedMetricName("listeners", "connections.idle.closed",
        "port", String.valueOf(port)));
  }

  public RawLogsIngester withMaxLength(int maxLength) {
    this.maxLength = maxLength;
    return this;
  }

  public RawLogsIngester withChannelIdleTimeout(int channelIdleTimeout) {
    this.channelIdleTimeout = channelIdleTimeout;
    return this;
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
      connectionsAccepted.inc();
      ChannelPipeline channelPipeline = ch.pipeline();
      channelPipeline.addLast(LineBasedFrameDecoder.class.getName(), new LineBasedFrameDecoder(maxLength));
      channelPipeline.addLast(StringDecoder.class.getName(), new StringDecoder(CharsetUtil.UTF_8));
      channelPipeline.addLast("logsIngestionHandler", new SimpleChannelInboundHandler<String>() {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
          received.inc();
          ingestLog(ctx, msg);
        }
      });
      channelPipeline.addLast("idleStateHandler", new IdleStateHandler(channelIdleTimeout, 0, 0));
      channelPipeline.addLast("idleChannelTerminator", new ChannelDuplexHandler() {
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx,
                                       Object evt) throws Exception {
          if (evt instanceof IdleStateEvent) {
            if (((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
              connectionsIdleClosed.inc();
              logger.info("Closing idle connection to raw logs client, inactivity timeout " +
                  channelIdleTimeout + "s expired: " + ctx.channel());
              ctx.close();
            }
          }
        }
      });
    }
  }

}
