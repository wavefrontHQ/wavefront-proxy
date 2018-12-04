package com.wavefront.agent.listeners;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.Clock;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportableEntityDecoder;

import org.apache.commons.lang.StringUtils;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  private static final Pattern PATTERN_CHECKIN = Pattern.compile("/api/daemon/(.*)/checkin");
  private static final Pattern PATTERN_PUSHDATA = Pattern.compile("/api/daemon/(.*)/pushdata/(.*)");

  private Cache<String, String> proxyTokenCache = Caffeine.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .maximumSize(10_000)
      .build();

  public RelayPortUnificationHandler(final String handle,
                                     final TokenAuthenticator tokenAuthenticator,
                                     final Map<ReportableEntityType, ReportableEntityDecoder> decoders,
                                     final ReportableEntityHandlerFactory handlerFactory,
                                     @Nullable final ReportableEntityPreprocessor preprocessor) {
    super(handle, tokenAuthenticator, decoders, handlerFactory, null, preprocessor);
  }

  @Override
  protected boolean authorized(final ChannelHandlerContext ctx, final FullHttpRequest request) {
    if (tokenAuthenticator.authRequired()) {
      String token = extractToken(ctx, request);

      URI uri = parseUri(ctx, request);
      if (uri == null) return false;

      Matcher patternPushDataMatcher = PATTERN_PUSHDATA.matcher(uri.getPath());
      if (patternPushDataMatcher.matches()) {
        // extract proxy ID from the URL and get actual token from cache
        token = proxyTokenCache.getIfPresent(patternPushDataMatcher.replaceAll("$1"));
      }

      if (!tokenAuthenticator.authorize(token)) { // 401 if no token or auth fails
        writeHttpResponse(ctx, HttpResponseStatus.UNAUTHORIZED, "401 Unauthorized\n");
        return false;
      }

      Matcher patternCheckinMatcher = PATTERN_CHECKIN.matcher(uri.getPath());
      if (patternCheckinMatcher.matches() && token != null) {
        String proxyId = patternCheckinMatcher.replaceAll("$1");
        logger.info("Caching auth token for proxy ID " + proxyId);
        proxyTokenCache.put(proxyId, token);
      }
    }
    return true;
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
