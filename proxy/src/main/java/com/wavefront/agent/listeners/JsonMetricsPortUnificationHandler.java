package com.wavefront.agent.listeners;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.Clock;
import com.wavefront.common.Pair;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportPointSerializer;
import com.wavefront.metrics.JsonMetricsParser;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;

import wavefront.report.ReportPoint;

/**
 * Agent-side JSON metrics endpoint.
 *
 * @author Clement Pang (clement@wavefront.com).
 * @author vasily@wavefront.com.
 */
@ChannelHandler.Sharable
public class JsonMetricsPortUnificationHandler extends PortUnificationHandler {
  private static final Logger logger = Logger.getLogger(JsonMetricsPortUnificationHandler.class.getCanonicalName());
  private static final Logger blockedPointsLogger = Logger.getLogger("RawBlockedPoints");
  private static final Set<String> STANDARD_PARAMS = ImmutableSet.of("h", "p", "d", "t");

  /**
   * The point handler that takes report metrics one data point at a time and handles batching and retries, etc
   */
  private final ReportableEntityHandler<ReportPoint> pointHandler;
  private final String prefix;
  private final String defaultHost;

  @Nullable
  private final ReportableEntityPreprocessor preprocessor;
  private final ObjectMapper jsonParser;

  /**
   * Create a new instance.
   *
   * @param handle          handle/port number.
   * @param authenticator   token authenticator.
   * @param handlerFactory  factory for ReportableEntityHandler objects.
   * @param prefix          metric prefix.
   * @param defaultHost     default host name to use, if none specified.
   * @param preprocessor    preprocessor.
   */
  @SuppressWarnings("unchecked")
  public JsonMetricsPortUnificationHandler(final String handle,
                                           final TokenAuthenticator authenticator,
                                           final ReportableEntityHandlerFactory handlerFactory,
                                           final String prefix,
                                           final String defaultHost,
                                           @Nullable final ReportableEntityPreprocessor preprocessor) {
    this(handle, authenticator, handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT, handle)),
        prefix, defaultHost, preprocessor);
  }


  @VisibleForTesting
  protected JsonMetricsPortUnificationHandler(final String handle,
                                              final TokenAuthenticator authenticator,
                                              final ReportableEntityHandler<ReportPoint> pointHandler,
                                              final String prefix,
                                              final String defaultHost,
                                              @Nullable final ReportableEntityPreprocessor preprocessor) {
    super(authenticator, handle, false, true);
    this.pointHandler = pointHandler;
    this.prefix = prefix;
    this.defaultHost = defaultHost;
    this.preprocessor = preprocessor;
    this.jsonParser = new ObjectMapper();
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest incomingRequest) {
    StringBuilder output = new StringBuilder();
    try {
      URI uri = parseUri(ctx, incomingRequest);
      Map<String, String> params = Arrays.stream(uri.getRawQuery().split("&")).
          map(x -> new Pair<>(x.split("=")[0].trim().toLowerCase(), x.split("=")[1])).
          collect(Collectors.toMap(k -> k._1, v -> v._2));

      String requestBody = incomingRequest.content().toString(CharsetUtil.UTF_8);

      Map<String, String> tags = Maps.newHashMap();
      params.entrySet().stream().filter(x -> !STANDARD_PARAMS.contains(x.getKey()) && x.getValue().length() > 0).
          forEach(x -> tags.put(x.getKey(), x.getValue()));
      List<ReportPoint> points = new ArrayList<>();
      Long timestamp;
      if (params.get("d") == null) {
        timestamp = Clock.now();
      } else {
        try {
          timestamp = Long.parseLong(params.get("d"));
        } catch (NumberFormatException e) {
          timestamp = Clock.now();
        }
      }
      String prefix = this.prefix == null ?
          params.get("p") :
          params.get("p") == null ? this.prefix : this.prefix + "." + params.get("p");
      String host = params.get("h") == null ? defaultHost : params.get("h");

      JsonNode metrics = jsonParser.readTree(requestBody);

      JsonMetricsParser.report("dummy", prefix, metrics, points, host, timestamp);
      for (ReportPoint point : points) {
        if (point.getAnnotations() == null || point.getAnnotations().isEmpty()) {
          point.setAnnotations(tags);
        } else {
          Map<String, String> newAnnotations = Maps.newHashMap(tags);
          newAnnotations.putAll(point.getAnnotations());
          point.setAnnotations(newAnnotations);
        }
        if (preprocessor != null) {
          if (!preprocessor.forReportPoint().filter(point)) {
            if (preprocessor.forReportPoint().getLastFilterResult() != null) {
              blockedPointsLogger.warning(ReportPointSerializer.pointToString(point));
            } else {
              blockedPointsLogger.info(ReportPointSerializer.pointToString(point));
            }
            pointHandler.reject((ReportPoint) null, preprocessor.forReportPoint().getLastFilterResult());
            continue;
          }
          preprocessor.forReportPoint().transform(point);
        }
        pointHandler.report(point);
      }
      writeHttpResponse(ctx, HttpResponseStatus.OK, output, incomingRequest);
    } catch (IOException e) {
      logWarning("WF-300: Error processing incoming JSON request", e, ctx);
      writeHttpResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, output, incomingRequest);
    }
  }

  /**
   * Handles an incoming plain text (string) message. Handles :
   */
  @Override
  protected void handlePlainTextMessage(final ChannelHandlerContext ctx,
                                        final String message) throws Exception {
    logWarning("WF-300: Plaintext protocol is not supported for JsonMetrics", null, ctx);
  }

  @Override
  protected void processLine(final ChannelHandlerContext ctx, final String message) {
    throw new UnsupportedOperationException("Invalid context for processLine");
  }
}
