package com.wavefront.agent;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.wavefront.agent.channel.ConnectionTrackingHandler;
import com.wavefront.agent.channel.IdleStateEventHandler;
import com.wavefront.agent.channel.PlainTextOrHttpFrameDecoder;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.timeout.IdleStateHandler;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Miscellaneous support methods for running Wavefront proxy.
 *
 * @author vasily@wavefront.com
 */
abstract class ProxyUtil {
  protected static final Logger logger = Logger.getLogger("proxy");

  private ProxyUtil() {
  }

  /**
   * Gets or creates  proxy id for this machine.
   *
   * @param proxyConfig proxy configuration
   * @return proxy ID
   */
  static UUID getOrCreateProxyId(ProxyConfig proxyConfig) {
    if (proxyConfig.isEphemeral()) {
      UUID proxyId = UUID.randomUUID(); // don't need to store one
      logger.info("Ephemeral proxy id created: " + proxyId);
      return proxyId;
    } else {
      return getOrCreateProxyIdFromFile(proxyConfig.getIdFile());
    }
  }

  /**
   * Read or create proxy id for this machine. Reads the UUID from specified file,
   * or from ~/.dshell/id if idFileName is null.
   *
   * @param idFileName file name to read proxy ID from.
   * @return proxy id
   */
  static UUID getOrCreateProxyIdFromFile(@Nullable String idFileName) {
    File proxyIdFile;
    UUID proxyId = UUID.randomUUID();
    if (idFileName != null) {
      proxyIdFile = new File(idFileName);
    } else {
      File userHome = new File(System.getProperty("user.home"));
      if (!userHome.exists() || !userHome.isDirectory()) {
        throw new RuntimeException("Cannot read from user.home, quitting");
      }
      File configDirectory = new File(userHome, ".dshell");
      if (configDirectory.exists()) {
        if (!configDirectory.isDirectory()) {
          throw new RuntimeException(configDirectory + " must be a directory!");
        }
      } else {
        if (!configDirectory.mkdir()) {
          throw new RuntimeException("Cannot create .dshell directory under " + userHome);
        }
      }
      proxyIdFile = new File(configDirectory, "id");
    }
    if (proxyIdFile.exists()) {
      if (proxyIdFile.isFile()) {
        try {
          proxyId = UUID.fromString(Objects.requireNonNull(Files.asCharSource(proxyIdFile,
              Charsets.UTF_8).readFirstLine()));
          logger.info("Proxy Id read from file: " + proxyId);
        } catch (IllegalArgumentException ex) {
          throw new RuntimeException("Cannot read proxy id from " + proxyIdFile +
              ", content is malformed");
        } catch (IOException e) {
          throw new RuntimeException("Cannot read from " + proxyIdFile, e);
        }
      } else {
        throw new RuntimeException(proxyIdFile + " is not a file!");
      }
    } else {
      logger.info("Proxy Id created: " + proxyId);
      try {
        Files.asCharSink(proxyIdFile, Charsets.UTF_8).write(proxyId.toString());
      } catch (IOException e) {
        throw new RuntimeException("Cannot write to " + proxyIdFile);
      }
    }
    return proxyId;
  }

  /**
   * Create a {@link ChannelInitializer} with a single {@link ChannelHandler},
   * wrapped in {@link PlainTextOrHttpFrameDecoder}.
   *
   * @param channelHandler        handler
   * @param port                  port number.
   * @param messageMaxLength      maximum line length for line-based protocols.
   * @param httpRequestBufferSize maximum request size for HTTP POST.
   * @param idleTimeout           idle timeout in seconds.
   * @return channel initializer
   */
  static ChannelInitializer<SocketChannel> createInitializer(ChannelHandler channelHandler,
                                                             int port, int messageMaxLength,
                                                             int httpRequestBufferSize,
                                                             int idleTimeout,
                                                             Optional<SslContext> sslContext) {
    return createInitializer(ImmutableList.of(() -> new PlainTextOrHttpFrameDecoder(channelHandler,
        messageMaxLength, httpRequestBufferSize)), port, idleTimeout, sslContext);
  }

  /**
   * Create a {@link ChannelInitializer} with multiple dynamically created
   * {@link ChannelHandler} objects.
   *
   * @param channelHandlerSuppliers Suppliers of ChannelHandlers.
   * @param port                    port number.
   * @param idleTimeout             idle timeout in seconds.
   * @return channel initializer
   */
  static ChannelInitializer<SocketChannel> createInitializer(
      Iterable<Supplier<ChannelHandler>> channelHandlerSuppliers, int port, int idleTimeout,
      Optional<SslContext> sslContext) {
    String strPort = String.valueOf(port);
    ChannelHandler idleStateEventHandler = new IdleStateEventHandler(Metrics.newCounter(
        new TaggedMetricName("listeners", "connections.idle.closed", "port", strPort)));
    ChannelHandler connectionTracker = new ConnectionTrackingHandler(
        Metrics.newCounter(new TaggedMetricName("listeners", "connections.accepted", "port",
            strPort)),
        Metrics.newCounter(new TaggedMetricName("listeners", "connections.active", "port",
            strPort)));
    if (sslContext.isPresent()) {
      logger.info("TLS enabled on port: " + port);
    }
    return new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        sslContext.ifPresent(s -> pipeline.addLast(s.newHandler(ch.alloc())));
        pipeline.addFirst("idlehandler", new IdleStateHandler(idleTimeout, 0, 0));
        pipeline.addLast("idlestateeventhandler", idleStateEventHandler);
        pipeline.addLast("connectiontracker", connectionTracker);
        channelHandlerSuppliers.forEach(x -> pipeline.addLast(x.get()));
      }
    };
  }
}
