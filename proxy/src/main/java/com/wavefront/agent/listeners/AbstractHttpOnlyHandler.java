package com.wavefront.agent.listeners;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for HTTP-only listeners. */
@ChannelHandler.Sharable
public abstract class AbstractHttpOnlyHandler extends AbstractPortUnificationHandler {
  private static final Logger logger =
      LoggerFactory.getLogger(AbstractHttpOnlyHandler.class.getCanonicalName());

  /**
   * Create new instance.
   *
   * @param tokenAuthenticator {@link TokenAuthenticator} for incoming requests.
   * @param healthCheckManager shared health check endpoint handler.
   * @param port handle/port number.
   */
  public AbstractHttpOnlyHandler(
      @Nullable final TokenAuthenticator tokenAuthenticator,
      @Nullable final HealthCheckManager healthCheckManager,
      final int port) {
    super(tokenAuthenticator, healthCheckManager, port);
  }

  protected abstract void handleHttpMessage(
      final ChannelHandlerContext ctx, final FullHttpRequest request) throws URISyntaxException;

  /** Discards plaintext content. */
  @Override
  protected void handlePlainTextMessage(
      final ChannelHandlerContext ctx, @Nonnull final String message) {
    pointsDiscarded.get().inc();
    logger.warn("Input discarded: plaintext protocol is not supported on port " + port);
  }
}
