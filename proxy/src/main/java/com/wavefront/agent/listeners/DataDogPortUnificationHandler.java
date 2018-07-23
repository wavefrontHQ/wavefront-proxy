package com.wavefront.agent.listeners;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.PointHandlerImpl;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;
import wavefront.report.ReportPoint;

/**
 * This class handles an incoming message of either String or FullHttpRequest type.  All other types are ignored.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class DataDogPortUnificationHandler extends PortUnificationHandler {
  private static final Logger logger = Logger.getLogger(DataDogPortUnificationHandler.class.getCanonicalName());
  private static final Logger blockedPointsLogger = Logger.getLogger("RawBlockedPoints");
  private static final Pattern INVALID_METRIC_CHARACTERS = Pattern.compile("[^-_\\.\\dA-Za-z]");
  private static final Pattern INVALID_TAG_CHARACTERS = Pattern.compile("[^-_:\\.\\\\/\\dA-Za-z]");

  private volatile Histogram httpRequestSize;

  /**
   * The point handler that takes report metrics one data point at a time and handles batching and retries, etc
   */
  private final PointHandler pointHandler;

  @Nullable
  private final ReportableEntityPreprocessor preprocessor;

  public DataDogPortUnificationHandler(final PointHandler pointHandler,
                                       @Nullable final ReportableEntityPreprocessor preprocessor) {
    super();
    this.pointHandler = pointHandler;
    this.preprocessor = preprocessor;
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) {
    URI uri;
    StringBuilder output = new StringBuilder();
    boolean isKeepAlive = HttpUtil.isKeepAlive(request);
    if (httpRequestSize == null) { // doesn't have to be threadsafe
      httpRequestSize = Metrics.newHistogram(new TaggedMetricName("listeners", "http-requests.payload-points",
          "port", String.valueOf(((InetSocketAddress) ctx.channel().localAddress()).getPort())));
    }
    AtomicInteger pointsPerRequest = new AtomicInteger();

    try {
      uri = new URI(request.uri());
    } catch (URISyntaxException e) {
      writeExceptionText(e, output);
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, output, isKeepAlive);
      logWarning("WF-300: Request URI '" + request.uri() + "' cannot be parsed", e, ctx);
      return;
    }

    if (uri.getPath().equals("/api/v1/series") || uri.getPath().equals("/api/v1/series/")) {
      final ObjectMapper jsonTree = new ObjectMapper();
      HttpResponseStatus status;
      try {
        if (reportMetrics(jsonTree.readTree(request.content().toString(CharsetUtil.UTF_8)), pointsPerRequest)) {
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
      httpRequestSize.update(pointsPerRequest.intValue());
      writeHttpResponse(ctx, status, output, isKeepAlive);
    } else {
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, "Unsupported path", isKeepAlive);
      logWarning("WF-300: Unexpected path '" + request.uri() + "'", null, ctx);
    }
  }

  /**
   * Handles an incoming plain text (string) message. Handles :
   */
  @Override
  protected void handlePlainTextMessage(final ChannelHandlerContext ctx,
                                        final String message) throws Exception {
    if (message == null) {
      throw new IllegalArgumentException("Message cannot be null");
    }
    final ObjectMapper jsonTree = new ObjectMapper();
    try {
      reportMetrics(jsonTree.readTree(message), null);
    } catch (Exception e) {
      logWarning("WF-300: Unable to parse JSON on plaintext port", e, ctx);
    }
  }

  @Override
  protected void processLine(final ChannelHandlerContext ctx, final String message) {
    throw new UnsupportedOperationException("Invalid context for processLine");
  }

  /**
   * Parse the metrics JSON and report the metrics found.  There are 2 formats supported: - array of points - single
   * point
   *
   * @param metrics a DataDog-format payload
   * @param pointCounter counter to track the number of points processed in one request
   *
   * @return true if all metrics added successfully; false o/w
   * @see #reportMetric(JsonNode, AtomicInteger)
   */
  private boolean reportMetrics(final JsonNode metrics, @Nullable final AtomicInteger pointCounter) {
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
      if (!reportMetric(metric, pointCounter)) {
        successful = false;
      }
    }
    return successful;
  }

  /**
   * Parse the individual timeseries object and send the metric to on to the point handler.
   *
   * @param metric the JSON object representing a single metric
   * @param pointCounter counter to track the number of points processed in one request
   *
   * @return True if the metric was reported successfully; False o/w
   */
  private boolean reportMetric(final JsonNode metric, @Nullable final AtomicInteger pointCounter) {
    if (metric == null) {
      pointHandler.handleBlockedPoint("Skipping - series object null.");
      return false;
    }
    try {
      if (metric.get("metric") == null ) {
        pointHandler.handleBlockedPoint("Skipping - 'metric' field missing.");
        return false;
      }
      String metricName = INVALID_METRIC_CHARACTERS.matcher(metric.get("metric").textValue()).replaceAll("_");
      String hostName = metric.get("host") == null ? "unknown" : metric.get("host").textValue();
      JsonNode tagsNode = metric.get("tags");
      Map<String, String> tags = new HashMap<>();
      if (tagsNode != null) {
        for (JsonNode tag : tagsNode) {
          String tagKv = tag.asText();
          if (tagKv.indexOf(',') > 0) { // comma-delimited list of tags
            for (String item : tagKv.split(",")) {
              extractTag(item, tags);
            }
          } else {
            extractTag(tagKv, tags);
          }
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
          if (pointCounter != null) {
            pointCounter.incrementAndGet();
          }
        } else {
          pointHandler.handleBlockedPoint("WF-300: Inconsistent point value size (expected: 2)");
        }
      }
      return true;
    } catch (final Exception e) {
      logger.log(Level.WARNING, "WF-300: Failed to add metric", e);
      return false;
    }
  }

  private void extractTag(String input, final Map<String, String> tags) {
    int tagKvIndex = input.indexOf(':');
    if (tagKvIndex > 0) { // first character can't be ':' either
      String tagK = input.substring(0, tagKvIndex);
      if (tagK.toLowerCase().equals("source")) {
        tags.put("_source", input.substring(tagKvIndex + 1, input.length()));
      } else {
        tags.put(INVALID_TAG_CHARACTERS.matcher(tagK).replaceAll("_"), input.substring(tagKvIndex + 1, input.length()));
      }
    }

  }
}

