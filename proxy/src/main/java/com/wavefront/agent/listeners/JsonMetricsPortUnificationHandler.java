package com.wavefront.agent.listeners;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.Clock;
import com.wavefront.common.Pair;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.metrics.JsonMetricsParser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;

import wavefront.report.ReportPoint;

import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;

/**
 * Agent-side JSON metrics endpoint.
 *
 * @author Clement Pang (clement@wavefront.com).
 * @author vasily@wavefront.com.
 */
@ChannelHandler.Sharable
public class JsonMetricsPortUnificationHandler extends AbstractHttpOnlyHandler {
  private static final Set<String> STANDARD_PARAMS = ImmutableSet.of("h", "p", "d", "t");

  /**
   * The point handler that takes report metrics one data point at a time and handles batching and retries, etc
   */
  private final ReportableEntityHandler<ReportPoint, String> pointHandler;
  private final String prefix;
  private final String defaultHost;

  @Nullable
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final ObjectMapper jsonParser;

  /**
   * Create a new instance.
   *
   * @param handle             handle/port number.
   * @param authenticator      token authenticator.
   * @param healthCheckManager shared health check endpoint handler.
   * @param handlerFactory     factory for ReportableEntityHandler objects.
   * @param prefix             metric prefix.
   * @param defaultHost        default host name to use, if none specified.
   * @param preprocessor       preprocessor.
   */
  public JsonMetricsPortUnificationHandler(
      final String handle, final TokenAuthenticator authenticator,
      final HealthCheckManager healthCheckManager,
      final ReportableEntityHandlerFactory handlerFactory,
      final String prefix, final String defaultHost,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor) {
    this(handle, authenticator, healthCheckManager, handlerFactory.getHandler(
        HandlerKey.of(ReportableEntityType.POINT, handle)), prefix, defaultHost, preprocessor);
  }

  @VisibleForTesting
  protected JsonMetricsPortUnificationHandler(
      final String handle, final TokenAuthenticator authenticator,
      final HealthCheckManager healthCheckManager,
      final ReportableEntityHandler<ReportPoint, String> pointHandler,
      final String prefix, final String defaultHost,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor) {
    super(authenticator, healthCheckManager, handle);
    this.pointHandler = pointHandler;
    this.prefix = prefix;
    this.defaultHost = defaultHost;
    this.preprocessorSupplier = preprocessor;
    this.jsonParser = new ObjectMapper();
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) throws URISyntaxException {
    StringBuilder output = new StringBuilder();
    try {
      URI uri = new URI(request.uri());
      Map<String, String> params = Arrays.stream(uri.getRawQuery().split("&")).
          map(x -> new Pair<>(x.split("=")[0].trim().toLowerCase(), x.split("=")[1])).
          collect(Collectors.toMap(k -> k._1, v -> v._2));

      String requestBody = request.content().toString(CharsetUtil.UTF_8);

      Map<String, String> tags = Maps.newHashMap();
      params.entrySet().stream().
          filter(x -> !STANDARD_PARAMS.contains(x.getKey()) && x.getValue().length() > 0).
          forEach(x -> tags.put(x.getKey(), x.getValue()));
      List<ReportPoint> points = new ArrayList<>();
      long timestamp;
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

      ReportableEntityPreprocessor preprocessor = preprocessorSupplier == null ?
          null : preprocessorSupplier.get();
      String[] messageHolder = new String[1];
      JsonMetricsParser.report("dummy", prefix, metrics, points, host, timestamp);
      for (ReportPoint point : points) {
        if (point.getAnnotations().isEmpty()) {
          point.setAnnotations(tags);
        } else {
          Map<String, String> newAnnotations = Maps.newHashMap(tags);
          newAnnotations.putAll(point.getAnnotations());
          point.setAnnotations(newAnnotations);
        }
        if (preprocessor != null) {
          preprocessor.forReportPoint().transform(point);
          if (!preprocessor.forReportPoint().filter(point, messageHolder)) {
            if (messageHolder[0] != null) {
              pointHandler.reject(point, messageHolder[0]);
            } else {
              pointHandler.block(point);
            }
            continue;
          }
        }
        pointHandler.report(point);
      }
      writeHttpResponse(ctx, HttpResponseStatus.OK, output, request);
    } catch (IOException e) {
      logWarning("WF-300: Error processing incoming JSON request", e, ctx);
      writeHttpResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, output, request);
    }
  }
}
