package com.wavefront.agent.listeners;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.channel.NoopHealthCheckManager;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;

import static com.wavefront.agent.channel.ChannelUtils.errorMessageWithRootCause;
import static com.wavefront.common.Utils.lazySupplier;
import static com.wavefront.agent.channel.ChannelUtils.formatErrorMessage;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * This is a base class for the majority of proxy's listeners. Handles an incoming message of
 * either String or FullHttpRequest type, all other types are ignored.
 * Has ability to support health checks and authentication of incoming HTTP requests.
 * Designed to be used with {@link com.wavefront.agent.channel.PlainTextOrHttpFrameDecoder}.
 *
 * @author vasily@wavefront.com
 */
@SuppressWarnings("SameReturnValue")
@ChannelHandler.Sharable
public abstract class AbstractPortUnificationHandler extends SimpleChannelInboundHandler<Object> {
  private static final Logger logger = Logger.getLogger(
      AbstractPortUnificationHandler.class.getCanonicalName());

  protected final Supplier<Histogram> httpRequestHandleDuration;
  protected final Supplier<Counter> requestsDiscarded;
  protected final Supplier<Counter> pointsDiscarded;
  protected final Supplier<Gauge<Long>> httpRequestsInFlightGauge;
  protected final AtomicLong httpRequestsInFlight = new AtomicLong();

  protected final String handle;
  protected final TokenAuthenticator tokenAuthenticator;
  protected final HealthCheckManager healthCheck;

  /**
   * Create new instance.
   *
   * @param tokenAuthenticator  {@link TokenAuthenticator} for incoming requests.
   * @param healthCheckManager  shared health check endpoint handler.
   * @param handle              handle/port number.
   */
  public AbstractPortUnificationHandler(@Nullable final TokenAuthenticator tokenAuthenticator,
                                        @Nullable final HealthCheckManager healthCheckManager,
                                        @Nullable final String handle) {
    this.tokenAuthenticator = ObjectUtils.firstNonNull(tokenAuthenticator,
        TokenAuthenticator.DUMMY_AUTHENTICATOR);
    this.healthCheck = healthCheckManager == null ?
        new NoopHealthCheckManager() : healthCheckManager;
    this.handle = firstNonNull(handle, "unknown");
    String portNumber = this.handle.replaceAll("^\\d", "");
    if (NumberUtils.isNumber(portNumber)) {
      healthCheck.setHealthy(Integer.parseInt(portNumber));
    }

    this.httpRequestHandleDuration = lazySupplier(() -> Metrics.newHistogram(
        new TaggedMetricName("listeners", "http-requests.duration-nanos", "port", this.handle)));
    this.requestsDiscarded = lazySupplier(() -> Metrics.newCounter(
        new TaggedMetricName("listeners", "http-requests.discarded", "port", this.handle)));
    this.pointsDiscarded = lazySupplier(() -> Metrics.newCounter(
        new TaggedMetricName("listeners", "items-discarded", "port", this.handle)));
    this.httpRequestsInFlightGauge = lazySupplier(() -> Metrics.newGauge(
        new TaggedMetricName("listeners", "http-requests.active", "port", this.handle),
        new Gauge<Long>() {
          @Override
          public Long value() {
            return httpRequestsInFlight.get();
          }
        }));
  }

  /**
   * Process incoming HTTP request.
   *
   * @param ctx     Channel handler's context
   * @param request HTTP request to process
   * @throws URISyntaxException in case of a malformed URL
   */
  protected abstract void handleHttpMessage(
      final ChannelHandlerContext ctx, final FullHttpRequest request) throws URISyntaxException;

  /**
   * Process incoming plaintext string.
   *
   * @param ctx     Channel handler's context
   * @param message Plaintext message to process
   */
  protected abstract void handlePlainTextMessage(final ChannelHandlerContext ctx,
                                                 @Nonnull final String message);

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (cause instanceof TooLongFrameException) {
      logWarning("Received line is too long, consider increasing pushListenerMaxReceivedLength",
          cause, ctx);
      return;
    }
    if (cause instanceof DecompressionException) {
      logWarning("Decompression error", cause, ctx);
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST,
          "Decompression error: " + cause.getMessage(), false);
      return;
    }
    if (cause instanceof IOException && cause.getMessage().contains("Connection reset by peer")) {
      // These errors are caused by the client and are safe to ignore
      return;
    }
    logWarning("Handler failed", cause, ctx);
    logger.log(Level.WARNING, "Unexpected error: ", cause);
  }

  protected String extractToken(final FullHttpRequest request) {
    String token = firstNonNull(request.headers().getAsString("X-AUTH-TOKEN"),
        request.headers().getAsString("Authorization"), "").replaceAll("^Bearer ", "").trim();
    Optional<NameValuePair> tokenParam = URLEncodedUtils.parse(URI.create(request.uri()),
        CharsetUtil.UTF_8).stream().filter(x -> x.getName().equals("t") ||
        x.getName().equals("token") || x.getName().equals("api_key")).findFirst();
    if (tokenParam.isPresent()) {
      token = tokenParam.get().getValue();
    }
    return token;
  }

  protected boolean authorized(final ChannelHandlerContext ctx, final FullHttpRequest request) {
    if (tokenAuthenticator.authRequired()) {
      String token = extractToken(request);
      if (!tokenAuthenticator.authorize(token)) { // 401 if no token or auth fails
        writeHttpResponse(ctx, HttpResponseStatus.UNAUTHORIZED, "401 Unauthorized\n", request);
        return false;
      }
    }
    return true;
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final Object message) {
    if (message instanceof String) {
      try {
        if (tokenAuthenticator.authRequired()) {
          // plaintext is disabled with auth enabled
          pointsDiscarded.get().inc();
          logger.warning("Input discarded: plaintext protocol is not supported on port " +
              handle + " (authentication enabled)");
          return;
        }
        handlePlainTextMessage(ctx, (String) message);
      } catch (final Exception e) {
        e.printStackTrace();
        logWarning("Failed to handle message", e, ctx);
      }
    } else if (message instanceof FullHttpRequest) {
      FullHttpRequest request = (FullHttpRequest) message;
      try {
        HttpResponse healthCheckResponse = healthCheck.getHealthCheckResponse(ctx, request);
        if (healthCheckResponse != null) {
          writeHttpResponse(ctx, healthCheckResponse, request);
          return;
        }
        if (!getHttpEnabled()) {
          requestsDiscarded.get().inc();
          logger.warning("Inbound HTTP request discarded: HTTP disabled on port " + handle);
          return;
        }
        if (authorized(ctx, request)) {
          httpRequestsInFlightGauge.get();
          httpRequestsInFlight.incrementAndGet();
          long startTime = System.nanoTime();
          try {
            handleHttpMessage(ctx, request);
          } finally {
            httpRequestsInFlight.decrementAndGet();
          }
          httpRequestHandleDuration.get().update(System.nanoTime() - startTime);
        }
      } catch (URISyntaxException e) {
        writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, errorMessageWithRootCause(e),
            request);
        logger.warning(formatErrorMessage("WF-300: Request URI '" + request.uri() +
            "' cannot be parsed", e, ctx));
      } catch (final Exception e) {
        e.printStackTrace();
        logWarning("Failed to handle message", e, ctx);
      }
    } else {
      logWarning("Received unexpected message type " +
          (message == null ? "" : message.getClass().getName()), null, ctx);
    }
  }

  /**
   * Checks whether HTTP protocol is enabled on this port
   *
   * @return whether HTTP protocol is enabled
   */
  protected boolean getHttpEnabled() {
    return true;
  }

  /**
   * Log a detailed error message with remote IP address
   *
   * @param message   the error message
   * @param e         the exception (optional) that caused the message to be blocked
   * @param ctx       ChannelHandlerContext (optional) to extract remote client ip
   */
  protected void logWarning(final String message,
                            @Nullable final Throwable e,
                            @Nullable final ChannelHandlerContext ctx) {
    logger.warning(formatErrorMessage(message, e, ctx));
  }
}
