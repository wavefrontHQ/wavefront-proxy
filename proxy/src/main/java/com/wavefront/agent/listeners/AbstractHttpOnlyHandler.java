package com.wavefront.agent.listeners;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;

import java.net.URISyntaxException;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;

/**
 * Base class for HTTP-only listeners.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public abstract class AbstractHttpOnlyHandler extends AbstractPortUnificationHandler {
  private static final Logger logger =
      Logger.getLogger(AbstractHttpOnlyHandler.class.getCanonicalName());

  /**
   * Create new instance.
   *
   * @param tokenAuthenticator {@link TokenAuthenticator} for incoming requests.
   * @param healthCheckManager shared health check endpoint handler.
   * @param handle             handle/port number.
   */
  public AbstractHttpOnlyHandler(@Nullable final TokenAuthenticator tokenAuthenticator,
                                 @Nullable final HealthCheckManager healthCheckManager,
                                 @Nullable final String handle) {
    super(tokenAuthenticator, healthCheckManager, handle);
  }

  protected abstract void handleHttpMessage(
      final ChannelHandlerContext ctx, final FullHttpRequest request) throws URISyntaxException;

  /**
   * Discards plaintext content.
   */
  @Override
  protected void handlePlainTextMessage(final ChannelHandlerContext ctx,
                                        @Nonnull final String message) {
    pointsDiscarded.get().inc();
    logger.warning("Input discarded: plaintext protocol is not supported on port " + handle);
  }
}
