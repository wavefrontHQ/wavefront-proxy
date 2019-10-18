package com.wavefront.agent.channel;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

/**
 * A collection of helper methods around Netty channels.
 *
 * @author vasily@wavefront.com
 */
public abstract class ChannelUtils {
  private static final Logger logger = Logger.getLogger(ChannelUtils.class.getCanonicalName());

  /**
   * Create a detailed error message from an exception, including current handle (port).
   *
   * @param message   the error message
   * @param e         the exception (optional) that caused the error
   * @param ctx       ChannelHandlerContext (optional) to extract remote client ip
   *
   * @return formatted error message
   */
  public static String formatErrorMessage(final String message,
                                          @Nullable final Throwable e,
                                          @Nullable final ChannelHandlerContext ctx) {
    StringBuilder errMsg = new StringBuilder(message);
    if (ctx != null) {
      errMsg.append("; remote: ");
      errMsg.append(getRemoteName(ctx));
    }
    if (e != null) {
      errMsg.append("; ");
      writeExceptionText(e, errMsg);
    }
    return errMsg.toString();
  }

  /**
   * Writes a HTTP response to channel.
   *
   * @param ctx      Channel handler context
   * @param status   HTTP status to return with the response
   * @param contents Response body payload (JsonNode or CharSequence)
   */
  public static void writeHttpResponse(final ChannelHandlerContext ctx,
                                       final HttpResponseStatus status,
                                       final Object /* JsonNode | CharSequence */ contents) {
    writeHttpResponse(ctx, status, contents, false);
  }

  /**
   * Writes a HTTP response to channel.
   *
   * @param ctx      Channel handler context
   * @param status   HTTP status to return with the response
   * @param contents Response body payload (JsonNode or CharSequence)
   * @param request  Incoming request (used to get keep-alive header)
   */
  public static void writeHttpResponse(final ChannelHandlerContext ctx,
                                       final HttpResponseStatus status,
                                       final Object /* JsonNode | CharSequence */ contents,
                                       final FullHttpRequest request) {
    writeHttpResponse(ctx, status, contents, HttpUtil.isKeepAlive(request));
  }

  /**
   * Writes a HTTP response to channel.
   *
   * @param ctx       Channel handler context
   * @param status    HTTP status to return with the response
   * @param contents  Response body payload (JsonNode or CharSequence)
   * @param keepAlive Keep-alive requested
   */
  public static void writeHttpResponse(final ChannelHandlerContext ctx,
                                       final HttpResponseStatus status,
                                       final Object /* JsonNode | CharSequence */ contents,
                                       boolean keepAlive) {
    final FullHttpResponse response;
    if (contents instanceof JsonNode) {
      response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
          Unpooled.copiedBuffer(contents.toString(), CharsetUtil.UTF_8));
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    } else if (contents instanceof CharSequence) {
      response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
          Unpooled.copiedBuffer((CharSequence) contents, CharsetUtil.UTF_8));
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
    } else {
      throw new IllegalArgumentException("Unexpected response content type, JsonNode or " +
          "CharSequence expected: " + contents.getClass().getName());
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
   * Write detailed exception text to a StringBuilder.
   *
   * @param e   Exceptions thrown
   * @param msg StringBuilder to write message to
   */
  public static void writeExceptionText(@Nonnull final Throwable e, @Nonnull StringBuilder msg) {
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
   *
   * @param ctx  Channel handler context
   * @return remote client's address in a string form
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

  /**
   * Attempt to parse request URI. Returns HTTP 400 to the client if unsuccessful.
   *
   * @param ctx     Channel handler's context.
   * @param request HTTP request.
   * @return parsed URI.
   */
  public static URI parseUri(final ChannelHandlerContext ctx, FullHttpRequest request) {
    try {
      return new URI(request.uri());
    } catch (URISyntaxException e) {
      StringBuilder output = new StringBuilder();
      writeExceptionText(e, output);
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, output, request);
      logger.warning(formatErrorMessage("WF-300: Request URI '" + request.uri() +
          "' cannot be parsed", e, ctx));
      return null;
    }
  }
}
