package com.wavefront.agent.listeners;

import static com.wavefront.agent.channel.ChannelUtils.*;
import static com.wavefront.common.Utils.lazySupplier;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.channel.NoopHealthCheckManager;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a base class for the majority of proxy's listeners. Handles an incoming message of either
 * String or FullHttpRequest type, all other types are ignored. Has ability to support health checks
 * and authentication of incoming HTTP requests. Designed to be used with {@link
 * com.wavefront.agent.channel.PlainTextOrHttpFrameDecoder}.
 */
@SuppressWarnings("SameReturnValue")
@ChannelHandler.Sharable
public abstract class AbstractPortUnificationHandler extends SimpleChannelInboundHandler<Object> {
  private static final Logger logger =
      LoggerFactory.getLogger(AbstractPortUnificationHandler.class.getCanonicalName());

  protected final Supplier<Histogram> httpRequestHandleDuration;
  protected final Supplier<Counter> requestsDiscarded;
  protected final Supplier<Counter> pointsDiscarded;
  protected final Supplier<Gauge<Long>> httpRequestsInFlightGauge;
  protected final AtomicLong httpRequestsInFlight = new AtomicLong();

  protected final int port;
  protected final TokenAuthenticator tokenAuthenticator;
  protected final HealthCheckManager healthCheck;

  /**
   * Create new instance.
   *
   * @param tokenAuthenticator {@link TokenAuthenticator} for incoming requests.
   * @param healthCheckManager shared health check endpoint handler.
   * @param port handle/port number.
   */
  public AbstractPortUnificationHandler(
      @Nullable final TokenAuthenticator tokenAuthenticator,
      @Nullable final HealthCheckManager healthCheckManager,
      final int port) {
    this.tokenAuthenticator =
        ObjectUtils.firstNonNull(tokenAuthenticator, TokenAuthenticator.DUMMY_AUTHENTICATOR);
    this.healthCheck =
        healthCheckManager == null ? new NoopHealthCheckManager() : healthCheckManager;
    this.port = port;
    healthCheck.setHealthy(this.port);

    this.httpRequestHandleDuration =
        lazySupplier(
            () ->
                Metrics.newHistogram(
                    new TaggedMetricName(
                        "listeners",
                        "http-requests.duration-nanos",
                        "port",
                        String.valueOf(this.port))));
    this.requestsDiscarded =
        lazySupplier(
            () ->
                Metrics.newCounter(
                    new TaggedMetricName(
                        "listeners",
                        "http-requests.discarded",
                        "port",
                        String.valueOf(this.port))));
    this.pointsDiscarded =
        lazySupplier(
            () ->
                Metrics.newCounter(
                    new TaggedMetricName(
                        "listeners", "items-discarded", "port", String.valueOf(this.port))));
    this.httpRequestsInFlightGauge =
        lazySupplier(
            () ->
                Metrics.newGauge(
                    new TaggedMetricName(
                        "listeners", "http-requests.active", "port", String.valueOf(this.port)),
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
   * @param ctx Channel handler's context
   * @param request HTTP request to process
   * @throws URISyntaxException in case of a malformed URL
   */
  protected abstract void handleHttpMessage(
      final ChannelHandlerContext ctx, final FullHttpRequest request) throws URISyntaxException;

  /**
   * Process incoming plaintext string.
   *
   * @param ctx Channel handler's context
   * @param message Plaintext message to process
   */
  protected abstract void handlePlainTextMessage(
      final ChannelHandlerContext ctx, @Nonnull final String message);

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (cause instanceof TooLongFrameException) {
      logWarning(
          "Received line is too long, consider increasing pushListenerMaxReceivedLength",
          cause,
          ctx);
      return;
    }
    if (cause instanceof DecompressionException) {
      logWarning("Decompression error", cause, ctx);
      writeHttpResponse(
          ctx, HttpResponseStatus.BAD_REQUEST, "Decompression error: " + cause.getMessage(), false);
      return;
    }
    if (cause instanceof IOException && cause.getMessage().contains("Connection reset by peer")) {
      // These errors are caused by the client and are safe to ignore
      return;
    }
    logWarning("Handler failed", cause, ctx);
    logger.warn("Unexpected error: ", cause);
  }

  protected String extractToken(final FullHttpRequest request) {
    String token =
        firstNonNull(
                request.headers().getAsString("X-AUTH-TOKEN"),
                request.headers().getAsString("Authorization"),
                "")
            .replaceAll("^Bearer ", "")
            .trim();
    Optional<NameValuePair> tokenParam =
        URLEncodedUtils.parse(URI.create(request.uri()), CharsetUtil.UTF_8).stream()
            .filter(
                x ->
                    x.getName().equals("t")
                        || x.getName().equals("token")
                        || x.getName().equals("api_key"))
            .findFirst();
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
          logger.warn(
              "Input discarded: plaintext protocol is not supported on port "
                  + port
                  + " (authentication enabled)");
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
          logger.warn("Inbound HTTP request discarded: HTTP disabled on port " + port);
          return;
        }
        if (authorized(ctx, request)) {
          httpRequestsInFlightGauge.get();
          httpRequestsInFlight.incrementAndGet();
          long startTime = System.nanoTime();
          if (request.method() == HttpMethod.OPTIONS) {
            writeHttpResponse(
                ctx,
                new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT, Unpooled.EMPTY_BUFFER),
                request);
            return;
          }
          try {
            handleHttpMessage(ctx, request);
          } finally {
            httpRequestsInFlight.decrementAndGet();
          }
          httpRequestHandleDuration.get().update(System.nanoTime() - startTime);
        }
      } catch (URISyntaxException e) {
        writeHttpResponse(
            ctx, HttpResponseStatus.BAD_REQUEST, errorMessageWithRootCause(e), request);
        logger.warn(
            formatErrorMessage(
                "WF-300: Request URI '" + request.uri() + "' cannot be parsed", e, ctx));
      } catch (final Exception e) {
        e.printStackTrace();
        logWarning("Failed to handle message", e, ctx);
      }
    } else {
      logWarning(
          "Received unexpected message type "
              + (message == null ? "" : message.getClass().getName()),
          null,
          ctx);
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
   * @param message the error message
   * @param e the exception (optional) that caused the message to be blocked
   * @param ctx ChannelHandlerContext (optional) to extract remote client ip
   */
  protected void logWarning(
      final String message,
      @Nullable final Throwable e,
      @Nullable final ChannelHandlerContext ctx) {
    if (logger.isDebugEnabled() && (e != null)) {
      logger.warn(formatErrorMessage(message, e, ctx), e);
    } else {
      logger.warn(formatErrorMessage(message, e, ctx));
    }
  }
}
