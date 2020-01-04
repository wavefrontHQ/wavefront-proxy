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
import io.netty.handler.timeout.IdleStateHandler;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Objects;
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
        logger.severe("Cannot read from user.home, quitting");
        System.exit(1);
      }
      File configDirectory = new File(userHome, ".dshell");
      if (configDirectory.exists()) {
        if (!configDirectory.isDirectory()) {
          logger.severe(configDirectory + " must be a directory!");
          System.exit(1);
        }
      } else {
        if (!configDirectory.mkdir()) {
          logger.severe("Cannot create .dshell directory under " + userHome);
          System.exit(1);
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
          logger.severe("Cannot read proxy id from " + proxyIdFile +
              ", content is malformed");
          System.exit(1);
        } catch (IOException e) {
          logger.log(Level.SEVERE, "Cannot read from " + proxyIdFile, e);
          System.exit(1);
        }
      } else {
        logger.severe(proxyIdFile + " is not a file!");
        System.exit(1);
      }
    } else {
      logger.info("Proxy Id created: " + proxyId);
      try {
        Files.asCharSink(proxyIdFile, Charsets.UTF_8).write(proxyId.toString());
      } catch (IOException e) {
        logger.severe("Cannot write to " + proxyIdFile);
        System.exit(1);
      }
    }
    return proxyId;
  }

  /**
   * Return a unique process identifier used to prevent collisions in ~proxy metrics.
   * Try to extract system PID from RuntimeMXBean name string (usually in the
   * "11111@hostname" format). If it's not parsable or an extracted PID is too low,
   * for example, when running in a containerized environment, chances of ID collision
   * are much higher, so we use a random 32bit hex string instead.
   *
   * @return unique process identifier string
   */
  static String getProcessId() {
    try {
      final String runtime = ManagementFactory.getRuntimeMXBean().getName();
      if (runtime.indexOf("@") >= 1) {
        long id = Long.parseLong(runtime.substring(0, runtime.indexOf("@")));
        if (id > 1000) {
          return Long.toString(id);
        }
      }
    } catch (Exception e) {
      // can't resolve process ID, fall back to using random ID
    }
    return Integer.toHexString((int) (Math.random() * Integer.MAX_VALUE));
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
                                                             int idleTimeout) {
    return createInitializer(ImmutableList.of(() -> new PlainTextOrHttpFrameDecoder(channelHandler,
        messageMaxLength, httpRequestBufferSize)), port, idleTimeout);
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
      Iterable<Supplier<ChannelHandler>> channelHandlerSuppliers, int port, int idleTimeout) {
    String strPort = String.valueOf(port);
    ChannelHandler idleStateEventHandler = new IdleStateEventHandler(Metrics.newCounter(
        new TaggedMetricName("listeners", "connections.idle.closed", "port", strPort)));
    ChannelHandler connectionTracker = new ConnectionTrackingHandler(
        Metrics.newCounter(new TaggedMetricName("listeners", "connections.accepted", "port",
            strPort)),
        Metrics.newCounter(new TaggedMetricName("listeners", "connections.active", "port",
            strPort)));
    return new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addFirst("idlehandler", new IdleStateHandler(idleTimeout, 0, 0));
        pipeline.addLast("idlestateeventhandler", idleStateEventHandler);
        pipeline.addLast("connectiontracker", connectionTracker);
        channelHandlerSuppliers.forEach(x -> pipeline.addLast(x.get()));
      }
    };
  }
}
