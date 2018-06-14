package com.wavefront.agent;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.InetSocketAddress;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpVersion;

/**
 * This class handles an incoming message of either String or FullHttpRequest type.  All other types are ignored. This
 * will likely be passed to the PlainTextOrHttpFrameDecoder as the handler for messages.
 *
 * @author vasily@wavefront.com
 */
abstract class PortUnificationHandler extends SimpleChannelInboundHandler<Object> {
  private static final Logger logger = Logger.getLogger(
      PortUnificationHandler.class.getCanonicalName());

  PortUnificationHandler() {}

  /**
   * Handles an incoming HTTP message.
   */
  protected abstract void handleHttpMessage(final ChannelHandlerContext ctx,
                                            final FullHttpRequest message) throws Exception;

  /**
   * Handles an incoming plain text (string) message.
   */
  protected abstract void handlePlainTextMessage(final ChannelHandlerContext ctx,
                                                 final String message) throws Exception;

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    logWarning("Handler failed", cause, ctx);
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final Object message) {
    try {
      if (message != null) {
        if (message instanceof String) {
          handlePlainTextMessage(ctx, (String) message);
        } else if (message instanceof FullHttpRequest) {
          handleHttpMessage(ctx, (FullHttpRequest) message);
        } else {
          logWarning("Received unexpected message type " + message.getClass().getName(), null, ctx);
        }
      }
    } catch (final Exception e) {
      logWarning("Failed to handle message", e, ctx);
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      if (((IdleStateEvent) evt).state() == IdleState.READER_IDLE) { // close idle connections
        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        InetSocketAddress localAddress = (InetSocketAddress) ctx.channel().localAddress();
        logger.info("Closing idle connection on port " + localAddress.getPort() +
            ", remote address: " + remoteAddress.getAddress().getHostAddress());
        ctx.channel().close();
      }
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
    StringBuilder errMsg = new StringBuilder(message);
    errMsg.append("; remote: ");
    errMsg.append(getRemoteName(ctx));
    if (e != null) {
      errMsg.append("; ");
      writeExceptionText(e, errMsg);
    }
    logger.warning(errMsg.toString());
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

