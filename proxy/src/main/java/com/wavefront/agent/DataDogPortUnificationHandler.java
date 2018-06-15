package com.wavefront.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wavefront.agent.preprocessor.PointPreprocessor;
import com.wavefront.common.Clock;
import com.wavefront.ingester.OpenTSDBDecoder;
import com.wavefront.metrics.JsonMetricsParser;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;
import wavefront.report.ReportPoint;

/**
 * This class handles an incoming message of either String or FullHttpRequest type.  All other types are ignored. This
 * will likely be passed to the PlainTextOrHttpFrameDecoder as the handler for messages.
 *
 * @author Mike McLaughlin (mike@wavefront.com)
 */
class DataDogPortUnificationHandler extends PortUnificationHandler {
  private static final Logger logger = Logger.getLogger(
      DataDogPortUnificationHandler.class.getCanonicalName());
  private static final Logger blockedPointsLogger = Logger.getLogger("RawBlockedPoints");

  /**
   * The point handler that takes report metrics one data point at a time and handles batching and retries, etc
   */
  private final PointHandler pointHandler;

  /**
   * OpenTSDB decoder object
   */
  private final OpenTSDBDecoder decoder;

  @Nullable
  private final PointPreprocessor preprocessor;

  DataDogPortUnificationHandler(final OpenTSDBDecoder decoder,
                                final PointHandler pointHandler,
                                @Nullable final PointPreprocessor preprocessor) {
    super();
    this.decoder = decoder;
    this.pointHandler = pointHandler;
    this.preprocessor = preprocessor;
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) {
    URI uri;
    StringBuilder output = new StringBuilder();
    boolean isKeepAlive = HttpUtil.isKeepAlive(request);

    try {
      uri = new URI(request.uri());
    } catch (URISyntaxException e) {
      writeExceptionText(e, output);
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, output, isKeepAlive);
      logWarning("WF-300: Request URI '" + request.uri() + "' cannot be parsed", e, ctx);
      return;
    }

    if (uri.getPath().equals("/api/v1/series")) {
      final ObjectMapper jsonTree = new ObjectMapper();
      HttpResponseStatus status;
      try {
        if (reportMetrics(jsonTree.readTree(request.content().toString(CharsetUtil.UTF_8)))) {
          status = HttpResponseStatus.NO_CONTENT;
        } else {
          status = HttpResponseStatus.BAD_REQUEST;
          output.append("At least one data point had error.");
        }
      } catch (Exception e) {
        status = HttpResponseStatus.BAD_REQUEST;
        writeExceptionText(e, output);
        logWarning("WF-300: Failed to handle /api/v1/series request", e, ctx);
      }
      writeHttpResponse(ctx, status, output, isKeepAlive);
    } else {
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, "Unsupported path", isKeepAlive);
      logWarning("WF-300: Unexpected path '" + request.uri() + "'", null, ctx);
    }
  }

  /**
   * Handles an incoming plain text (string) message. Handles :
   */
  protected void handlePlainTextMessage(final ChannelHandlerContext ctx,
                                        final String message) throws Exception {
    if (message == null) {
      throw new IllegalArgumentException("Message cannot be null");
    }
    final ObjectMapper jsonTree = new ObjectMapper();
    try {
      reportMetrics(jsonTree.readTree(message));
    } catch (Exception e) {
      logWarning("WF-300: Unable to parse JSON on plaintext port", e, ctx);
    }
  }

  /**
   * Parse the metrics JSON and report the metrics found.  There are 2 formats supported: - array of points - single
   * point
   *
   * @param metrics a DataDog-format payload
   * @return true if all metrics added successfully; false o/w
   * @see #reportMetric(JsonNode)
   */
  private boolean reportMetrics(final JsonNode metrics) {
    if (!metrics.isObject() || !metrics.has("series")) {
      pointHandler.handleBlockedPoint("WF-300: Payload missing 'series' field");
      return false;
    }
    JsonNode series = metrics.get("series");
    if (!series.isArray()) {
      pointHandler.handleBlockedPoint("WF-300: 'series' field must be an array");
      return false;
    }
    boolean successful = true;
    for (final JsonNode metric : series) {
      if (!reportMetric(metric)) {
        successful = false;
      }
    }
    return successful;
  }

  /**
   * Parse the individual timeseries object and send the metric to on to the point handler.
   *
   * @param metric the JSON object representing a single metric
   * @return True if the metric was reported successfully; False o/w
   * @see <a href="http://opentsdb.net/docs/build/html/api_http/put.html">OpenTSDB /api/put documentation</a>
   */
  private boolean reportMetric(final JsonNode metric) {
    try {
      String metricName = metric.get("metric").textValue();
      JsonNode hostNode = metric.get("host");
      String hostName = hostNode == null ? "default" : hostNode.textValue();
      JsonNode tagsNode = metric.get("tags");
      Map<String, String> tags = new HashMap<>();
      if (tagsNode != null) {
        for (JsonNode tag : tagsNode) {
          String[] tagKeyValuePairs = tag.textValue().split(":");
          tags.put(tagKeyValuePairs[0], tagKeyValuePairs[1]);
        }
      }
      JsonNode pointsNode = metric.get("points");
      if (pointsNode == null) {
        pointHandler.handleBlockedPoint("Skipping - 'points' field missing.");
        return false;
      }
      for (JsonNode node : pointsNode) {
        ReportPoint point;
        if (node.size() == 2) {
          point = ReportPoint.newBuilder().
              setTable("dummy").
              setMetric(metricName).
              setHost(hostName).
              setTimestamp(node.get(0).longValue() * 1000).
              setAnnotations(tags).
              setValue(node.get(1).isDouble() ? node.get(1).asDouble() : node.get(1).asLong()).
              build();
          if (preprocessor != null) {
            preprocessor.forReportPoint().transform(point);
            if (!preprocessor.forReportPoint().filter(point)) {
              if (preprocessor.forReportPoint().getLastFilterResult() != null) {
                blockedPointsLogger.warning(PointHandlerImpl.pointToString(point));
              } else {
                blockedPointsLogger.info(PointHandlerImpl.pointToString(point));
              }
              pointHandler.handleBlockedPoint(preprocessor.forReportPoint().getLastFilterResult());
              return false;
            }
          }
          pointHandler.reportPoint(point, null);
        } else {
          pointHandler.handleBlockedPoint("WF-300: Inconsistent point value size (expected: 2)");
        }
      }
      return true;
    } catch (final Exception e) {
      logWarning("WF-300: Failed to add metric", e, null);
      return false;
    }
  }
}

