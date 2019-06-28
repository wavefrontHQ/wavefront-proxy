package com.wavefront.agent.listeners;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.Clock;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportableEntityDecoder;

import org.apache.commons.lang.StringUtils;

import java.net.URI;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;

/**
 * Process incoming HTTP requests from other proxies (i.e. act as a relay for proxy chaining).
 * Supports metric and histogram data (no source tag or tracing support at this moment).
 * Intended for internal use.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class RelayPortUnificationHandler extends WavefrontPortUnificationHandler {
  private static final Logger logger = Logger.getLogger(RelayPortUnificationHandler.class.getCanonicalName());

  public RelayPortUnificationHandler(final String handle,
                                     final TokenAuthenticator tokenAuthenticator,
                                     final Map<ReportableEntityType, ReportableEntityDecoder> decoders,
                                     final ReportableEntityHandlerFactory handlerFactory,
                                     @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor) {
    super(handle, tokenAuthenticator, decoders, handlerFactory, null, preprocessor);
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) {
    StringBuilder output = new StringBuilder();

    URI uri = parseUri(ctx, request);
    if (uri == null) return;

    if (uri.getPath().startsWith("/api/daemon") && uri.getPath().endsWith("/checkin")) {
      // simulate checkin response for proxy chaining
      ObjectNode jsonResponse = JsonNodeFactory.instance.objectNode();
      jsonResponse.put("currentTime", Clock.now());
      jsonResponse.put("allowAnyHostKeys", true);
      writeHttpResponse(ctx, HttpResponseStatus.OK, jsonResponse, request);
      return;
    }

    HttpResponseStatus status;
    try {
      for (String line : StringUtils.split(request.content().toString(CharsetUtil.UTF_8), '\n')) {
        processLine(ctx, line.trim());
      }
      status = HttpResponseStatus.OK;
    } catch (Exception e) {
      status = HttpResponseStatus.BAD_REQUEST;
      writeExceptionText(e, output);
      logWarning("WF-300: Failed to handle HTTP POST", e, ctx);
    }
    writeHttpResponse(ctx, status, output, request);
  }
}
