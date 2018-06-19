package com.wavefront.agent;

import com.wavefront.agent.preprocessor.PointPreprocessor;
import com.wavefront.ingester.Decoder;
import com.wavefront.ingester.SourceTagDecoder;

import org.apache.commons.lang.StringUtils;

import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;

/**
 * Handle an incoming message of either String or FullHttpRequest type:
 * single point in a string or multiple points in the post body, newline-delimited
 */
class WavefrontPortUnificationHandler extends PortUnificationHandler {
  private static final Logger logger = Logger.getLogger(
      WavefrontPortUnificationHandler.class.getCanonicalName());

  /**
   * The point handler that takes report metrics one data point at a time and handles batching and retries, etc
   */
  private final PointHandler pointHandler;

  @Nullable
  private final SourceTagHandler metadataHandler;

  private final Decoder<String> decoder;

  @Nullable
  private final PointPreprocessor preprocessor;


  WavefrontPortUnificationHandler(final Decoder<String> decoder,
                                  final PointHandler pointHandler,
                                  @Nullable final PointPreprocessor preprocessor,
                                  @Nullable final SourceTagHandler metadataHandler) {
    this.decoder = decoder;
    this.pointHandler = pointHandler;
    this.metadataHandler = metadataHandler;
    this.preprocessor = preprocessor;
  }

  /**
   * Handles an incoming HTTP message. Accepts HTTP POST on all paths
   */
  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) {
    StringBuilder output = new StringBuilder();
    boolean isKeepAlive = HttpUtil.isKeepAlive(request);

    HttpResponseStatus status;
    try {
      for (String line : StringUtils.split(request.content().toString(CharsetUtil.UTF_8), '\n')) {
        processPointOrSourceTag(ctx, line);
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
   * Handles an incoming plain text (string) message.
   */
  @Override
  protected void handlePlainTextMessage(final ChannelHandlerContext ctx,
                                      final String message) throws Exception {
    if (message == null) {
      throw new IllegalArgumentException("Message cannot be null");
    }
    processPointOrSourceTag(ctx, message);
  }

  /**
   *
   * @param ctx      ChannelHandler context (to retrieve remote client's IP in case of errors)
   * @param message  line being processed
   */
  private void processPointOrSourceTag(final ChannelHandlerContext ctx, final String message) {
    if (metadataHandler != null && message != null) {
      String msg = message.trim();
      if (msg.startsWith(SourceTagDecoder.SOURCE_TAG) ||
          msg.startsWith(SourceTagDecoder.SOURCE_DESCRIPTION)) {
        metadataHandler.processSourceTag(msg);
        return;
      }
    }
    ChannelStringHandler.processPointLine(message, decoder, pointHandler, preprocessor, ctx);
  }
}

