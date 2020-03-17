package com.wavefront.agent.channel;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpResponse;
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
      errMsg.append(errorMessageWithRootCause(e));
    }
    return errMsg.toString();
  }

  /**
   * Writes HTTP response back to client.
   *
   * @param ctx      Channel handler context
   * @param status   HTTP status to return with the response
   * @param contents Response body payload (JsonNode or CharSequence)
   * @param request  Incoming request (used to get keep-alive header)
   */
  public static void writeHttpResponse(final ChannelHandlerContext ctx,
                                       final HttpResponseStatus status,
                                       final Object /* JsonNode | CharSequence */ contents,
                                       final HttpMessage request) {
    writeHttpResponse(ctx, status, contents, HttpUtil.isKeepAlive(request));
  }

  /**
   * Writes HTTP response back to client.
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
    writeHttpResponse(ctx, makeResponse(status, contents), keepAlive);
  }

  /**
   * Writes HTTP response back to client.
   *
   * @param ctx       Channel handler context.
   * @param response  HTTP response object.
   * @param request   HTTP request object (to extract keep-alive flag).
   */
  public static void writeHttpResponse(final ChannelHandlerContext ctx,
                                       final HttpResponse response,
                                       final HttpMessage request) {
    writeHttpResponse(ctx, response, HttpUtil.isKeepAlive(request));
  }

  /**
   * Writes HTTP response back to client.
   *
   * @param ctx       Channel handler context.
   * @param response  HTTP response object.
   * @param keepAlive Keep-alive requested.
   */
  public static void writeHttpResponse(final ChannelHandlerContext ctx,
                                       final HttpResponse response,
                                       boolean keepAlive) {
    // Decide whether to close the connection or not.
    if (keepAlive) {
      // Add keep alive header as per:
      // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      ctx.write(response);
    } else {
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
  }

  /**
   * Create {@link FullHttpResponse} based on provided status and body contents.
   *
   * @param status   response status.
   * @param contents response body.
   * @return http response object
   */
  public static HttpResponse makeResponse(final HttpResponseStatus status,
                                          final Object /* JsonNode | CharSequence */ contents) {
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
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
    return response;
  }

  /**
   * Write detailed exception text.
   *
   * @param e   Exceptions thrown
   * @return error message
   */
  public static String errorMessageWithRootCause(@Nonnull final Throwable e) {
    StringBuilder msg = new StringBuilder();
    final Throwable rootCause = Throwables.getRootCause(e);
    msg.append("reason: \"");
    msg.append(e.getMessage());
    msg.append("\"");
    if (rootCause != null && rootCause != e && rootCause.getMessage() != null) {
      msg.append(", root cause: \"");
      msg.append(rootCause.getMessage());
      msg.append("\"");
    }
    return msg.toString();
  }

  /**
   * Get remote client's address as string (without rDNS lookup) and local port
   *
   * @param ctx  Channel handler context
   * @return remote client's address in a string form
   */
  @Nonnull
  public static String getRemoteName(@Nullable final ChannelHandlerContext ctx) {
    if (ctx != null) {
      InetAddress remoteAddress = getRemoteAddress(ctx);
      InetSocketAddress localAddress = (InetSocketAddress) ctx.channel().localAddress();
      if (remoteAddress != null && localAddress != null) {
        return remoteAddress.getHostAddress() + " [" + localAddress.getPort() + "]";
      }
    }
    return "";
  }

  /**
   * Get {@link InetAddress} for the current channel.
   *
   * @param ctx Channel handler's context.
   * @return remote address
   */
  public static InetAddress getRemoteAddress(@Nonnull ChannelHandlerContext ctx) {
    InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
    return remoteAddress == null ? null : remoteAddress.getAddress();
  }

  /**
   * Get path from the URI (i.e. discard everything after "?").
   *
   * @param uri URI to get path from
   * @return path
   */
  public static String getPath(String uri) {
    int paramsDelimiter = uri.indexOf('?');
    return paramsDelimiter == -1 ? uri : uri.substring(0, paramsDelimiter);
  }

  /**
   * Parse query params into name/value pairs.
   *
   * @param uri URI to get query params from
   * @return list of {@link NameValuePair}
   */
  public static List<NameValuePair> getQueryParams(String uri) {
    int paramsDelimiter = uri.indexOf('?');
    return paramsDelimiter == -1 ? ImmutableList.of() :
        URLEncodedUtils.parse(uri.substring(paramsDelimiter + 1), CharsetUtil.UTF_8);
  }
}
