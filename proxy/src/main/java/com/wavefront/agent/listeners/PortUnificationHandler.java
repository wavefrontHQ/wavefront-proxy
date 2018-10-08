package com.wavefront.agent.listeners;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.databind.JsonNode;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;

import org.apache.commons.lang.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpVersion;

/**
 * This class handles an incoming message of either String or FullHttpRequest type.  All other types are ignored. This
 * will likely be passed to the PlainTextOrHttpFrameDecoder as the handler for messages.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public abstract class PortUnificationHandler extends SimpleChannelInboundHandler<Object> {
  private static final Logger logger = Logger.getLogger(
      PortUnificationHandler.class.getCanonicalName());

  protected volatile Histogram httpRequestHandleDuration;
  protected volatile Counter pointsDiscarded;

  protected final String handle;
  protected final TokenAuthenticator tokenAuthenticator;

  /**
   * Create new instance.
   *
   * @param tokenAuthenticator  tokenAuthenticator for incoming requests.
   * @param handle              handle/port number.
   */
  public PortUnificationHandler(@Nonnull TokenAuthenticator tokenAuthenticator, @Nullable final String handle) {
    this.tokenAuthenticator = tokenAuthenticator;
    this.handle = handle;
  }

  /**
   * Handles an incoming HTTP message. Accepts HTTP POST on all paths
   */
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) {
    StringBuilder output = new StringBuilder();
    boolean isKeepAlive = HttpUtil.isKeepAlive(request);

    if (tokenAuthenticator.authRequired()) {
      String token = null;
      String authorizationHeader = request.headers().getAsString("Authorization");
      if (authorizationHeader.startsWith("Bearer ")) {
        token = authorizationHeader.replace("Bearer ", "").trim();
      }
      URI requestUri;
      try {
        requestUri = new URI(request.uri());
      } catch (URISyntaxException e) {
        writeExceptionText(e, output);
        writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, output, isKeepAlive);
        logWarning("WF-300: Request URI '" + request.uri() + "' cannot be parsed", e, ctx);
        return;
      }
      Optional<NameValuePair> tokenParam = URLEncodedUtils.parse(requestUri, CharsetUtil.UTF_8).stream().
          filter(x -> x.getName().equals("t")).findFirst();
      if (tokenParam.isPresent()) {
        token = tokenParam.get().getValue();
      }
      if (!tokenAuthenticator.authorize(token)) { // 401 if no token or auth fails
        output.append("401 Unauthorized\n");
        writeHttpResponse(ctx, HttpResponseStatus.UNAUTHORIZED, output, isKeepAlive);
        return;
      }
    }

    HttpResponseStatus status;
    try {
      for (String line : StringUtils.split(request.content().toString(CharsetUtil.UTF_8), '\n')) {
        processLine(ctx, line.trim());
      }
      status = HttpResponseStatus.NO_CONTENT;
    } catch (Exception e) {
      status = HttpResponseStatus.BAD_REQUEST;
      writeExceptionText(e, output);
      logWarning("WF-300: Failed to handle HTTP POST", e, ctx);
    }
    writeHttpResponse(ctx, status, output, isKeepAlive);
  }

  /**
   * Handles an incoming plain text (string) message. By default simply passes a string to
   * {@link #processLine(ChannelHandlerContext, String)} method.
   */
  protected void handlePlainTextMessage(final ChannelHandlerContext ctx,
                                        final String message) throws Exception {
    if (message == null) {
      throw new IllegalArgumentException("Message cannot be null");
    }
    if (tokenAuthenticator.authRequired()) { // plaintext is disabled with auth enabled
      logger.warning("Point discarded: plaintext protocol disabled when authentication is enabled");
      if (pointsDiscarded == null) {
        pointsDiscarded = Metrics.newCounter(new TaggedMetricName("listeners", "items-discarded", "port", handle));
      }
      pointsDiscarded.inc();
      return;
    }
    processLine(ctx, message.trim());
  }

  protected abstract void processLine(final ChannelHandlerContext ctx, final String message);

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (cause instanceof TooLongFrameException) {
      logWarning("Received line is too long, consider increasing pushListenerMaxReceivedLength", cause, ctx);
    }
    if (cause instanceof DecompressionException) {
      logWarning("Decompression error", cause, ctx);
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, "Decompression error: " + cause.getMessage(), false);
      return;
    }
    if (cause instanceof IOException && cause.getMessage().contains("Connection reset by peer")) {
      // These errors are caused by the client and are safe to ignore
      return;
    }
    logWarning("Handler failed", cause, ctx);
    logger.log(Level.WARNING, "Unexpected error: ", cause);
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final Object message) {
    try {
      if (message != null) {
        if (message instanceof String) {
          handlePlainTextMessage(ctx, (String) message);
        } else if (message instanceof FullHttpRequest) {
          if (httpRequestHandleDuration == null) { // doesn't have to be threadsafe
            httpRequestHandleDuration = Metrics.newHistogram(new TaggedMetricName("listeners", "http-requests.duration",
                "port", String.valueOf(((InetSocketAddress) ctx.channel().localAddress()).getPort())));
          }
          long startTime = System.nanoTime();
          handleHttpMessage(ctx, (FullHttpRequest) message);
          httpRequestHandleDuration.update(System.nanoTime() - startTime);
        } else {
          logWarning("Received unexpected message type " + message.getClass().getName(), null, ctx);
        }
      }
    } catch (final Exception e) {
      logWarning("Failed to handle message", e, ctx);
    }
  }

  /**
   * Writes an HTTP response.
   */
  protected void writeHttpResponse(final ChannelHandlerContext ctx, final HttpResponseStatus status,
                                   final Object contents, boolean keepAlive) {
    final FullHttpResponse response;
    if (contents instanceof JsonNode) {
      response = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer(contents.toString(), CharsetUtil.UTF_8));
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    } else if (contents instanceof CharSequence) {
      response = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer((CharSequence) contents, CharsetUtil.UTF_8));
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
    } else {
      throw new IllegalArgumentException("Unexpected response content type, JsonNode or CharSequence expected: " +
          contents.getClass().getName());
    }

    // Decide whether to close the connection or not.
    if (keepAlive) {
      // Add 'Content-Length' header only for a keep-alive connection.
      response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
      // Add keep alive header as per:
      // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      ctx.write(response);
    } else {
      ctx.write(response);
      ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
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

  /**
   * Create a detailed error message from an exception.
   *
   * @param message   the error message
   * @param e         the exception (optional) that caused the error
   * @param ctx       ChannelHandlerContext (optional) to extract remote client ip
   *
   * @return formatted error message
   */
  protected String formatErrorMessage(final String message,
                                      @Nullable final Throwable e,
                                      @Nullable final ChannelHandlerContext ctx) {
    StringBuilder errMsg = new StringBuilder(message);
    errMsg.append("; remote: ");
    errMsg.append(getRemoteName(ctx));
    if (e != null) {
      errMsg.append("; ");
      writeExceptionText(e, errMsg);
    }
    return errMsg.toString();
  }

  /**
   * Create a error message from an exception.
   *
   * @param e   Exceptions thrown
   * @param msg StringBuilder to write message to
   */
  protected void writeExceptionText(@Nonnull final Throwable e, @Nonnull StringBuilder msg) {
    final Throwable rootCause = Throwables.getRootCause(e);
    msg.append("reason: \"");
    msg.append(e.getMessage());
    msg.append("\"");
    if (rootCause != null && rootCause != e && rootCause.getMessage() != null) {
      msg.append(", root cause: \"");
      msg.append(rootCause.getMessage());
      msg.append("\"");
    }
  }

  /**
   * Get remote client's address as string (without rDNS lookup) and local port
   */
  public static String getRemoteName(@Nullable final ChannelHandlerContext ctx) {
    if (ctx != null) {
      InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
      InetSocketAddress localAddress = (InetSocketAddress) ctx.channel().localAddress();
      if (remoteAddress != null && localAddress != null) {
        return remoteAddress.getAddress().getHostAddress() + " [" + localAddress.getPort() + "]";
      }
    }
    return "";
  }
}

