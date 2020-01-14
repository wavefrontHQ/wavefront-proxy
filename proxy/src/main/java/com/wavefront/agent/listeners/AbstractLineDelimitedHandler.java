package com.wavefront.agent.listeners;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;

import static com.wavefront.agent.channel.ChannelUtils.errorMessageWithRootCause;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static com.wavefront.agent.handlers.LineDelimitedUtils.splitPushData;

/**
 * Base class for all line-based protocols. Supports TCP line protocol as well as HTTP POST
 * with newline-delimited payload.
 *
 * @author vasily@wavefront.com.
 */
@ChannelHandler.Sharable
public abstract class AbstractLineDelimitedHandler extends AbstractPortUnificationHandler {

  /**
   * @param tokenAuthenticator {@link TokenAuthenticator} for incoming requests.
   * @param healthCheckManager shared health check endpoint handler.
   * @param handle             handle/port number.
   */
  public AbstractLineDelimitedHandler(@Nullable final TokenAuthenticator tokenAuthenticator,
                                      @Nullable final HealthCheckManager healthCheckManager,
                                      @Nullable final String handle) {
    super(tokenAuthenticator, healthCheckManager, handle);
  }

  /**
   * Handles an incoming HTTP message. Accepts HTTP POST on all paths
   */
  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) {
    StringBuilder output = new StringBuilder();
    HttpResponseStatus status;
    try {
      for (String line : splitPushData(request.content().toString(CharsetUtil.UTF_8))) {
        processLine(ctx, line.trim());
      }
      status = HttpResponseStatus.ACCEPTED;
    } catch (Exception e) {
      status = HttpResponseStatus.BAD_REQUEST;
      output.append(errorMessageWithRootCause(e));
      logWarning("WF-300: Failed to handle HTTP POST", e, ctx);
    }
    writeHttpResponse(ctx, status, output, request);
  }

  /**
   * Handles an incoming plain text (string) message. By default simply passes a string to
   * {@link #processLine(ChannelHandlerContext, String)} method.
   */
  @Override
  protected void handlePlainTextMessage(final ChannelHandlerContext ctx,
                                        final String message) {
    if (message == null) {
      throw new IllegalArgumentException("Message cannot be null");
    }
    processLine(ctx, message.trim());
  }

  /**
   * Process a single line for a line-based stream.
   *
   * @param ctx     Channel handler context.
   * @param message Message to process.
   */
  protected abstract void processLine(final ChannelHandlerContext ctx, final String message);
}
