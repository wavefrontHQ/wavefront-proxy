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
class OpenTSDBPortUnificationHandler extends PortUnificationHandler {
  private static final Logger logger = Logger.getLogger(
      OpenTSDBPortUnificationHandler.class.getCanonicalName());
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

  OpenTSDBPortUnificationHandler(final OpenTSDBDecoder decoder,
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

    if (uri.getPath().equals("/api/put")) {
      final ObjectMapper jsonTree = new ObjectMapper();
      HttpResponseStatus status;
      // from the docs:
      // The put endpoint will respond with a 204 HTTP status code and no content if all data points
      // were stored successfully. If one or more data points had an error, the API will return a 400.
      try {
        if (reportMetrics(jsonTree.readTree(request.content().toString(CharsetUtil.UTF_8)))) {
          status = HttpResponseStatus.NO_CONTENT;
        } else {
          // TODO: improve error message
          // http://opentsdb.net/docs/build/html/api_http/put.html#response
          // User should understand that successful points are processed and the reason for BAD_REQUEST
          // is due to at least one failure point.
          status = HttpResponseStatus.BAD_REQUEST;
          output.append("At least one data point had error.");
        }
      } catch (Exception e) {
        status = HttpResponseStatus.BAD_REQUEST;
        writeExceptionText(e, output);
        logWarning("WF-300: Failed to handle /api/put request", e, ctx);
      }
      writeHttpResponse(ctx, status, output, isKeepAlive);
    } else if (uri.getPath().equals("/api/version")) {
      // http://opentsdb.net/docs/build/html/api_http/version.html
      ObjectNode node = JsonNodeFactory.instance.objectNode();
      node.put("version", ResourceBundle.getBundle("build").getString("build.version"));
      writeHttpResponse(ctx, HttpResponseStatus.OK, node, isKeepAlive);
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
    if (message.startsWith("version")) {
      ChannelFuture f = ctx.writeAndFlush("Wavefront OpenTSDB Endpoint\n");
      if (!f.isSuccess()) {
        throw new Exception("Failed to write version response", f.cause());
      }
    } else {
      ChannelStringHandler.processPointLine(message, decoder, pointHandler, preprocessor, ctx);
    }
  }

  /**
   * Parse the metrics JSON and report the metrics found.  There are 2 formats supported: - array of points - single
   * point
   *
   * @param metrics an array of objects or a single object representing a metric
   * @return true if all metrics added successfully; false o/w
   * @see #reportMetric(JsonNode)
   */
  private boolean reportMetrics(final JsonNode metrics) {
    if (!metrics.isArray()) {
      return reportMetric(metrics);
    } else {
      boolean successful = true;
      for (final JsonNode metric : metrics) {
        if (!reportMetric(metric)) {
          successful = false;
        }
      }
      return successful;
    }
  }

  /**
   * Parse the individual metric object and send the metric to on to the point handler.
   *
   * @param metric the JSON object representing a single metric
   * @return True if the metric was reported successfully; False o/w
   * @see <a href="http://opentsdb.net/docs/build/html/api_http/put.html">OpenTSDB /api/put documentation</a>
   */
  private boolean reportMetric(final JsonNode metric) {
    try {
      String metricName = metric.get("metric").textValue();
      JsonNode tags = metric.get("tags");
      Map<String, String> wftags = JsonMetricsParser.makeTags(tags);

      String hostName;
      if (wftags.containsKey("host")) {
        hostName = wftags.get("host");
      } else if (wftags.containsKey("source")) {
        hostName = wftags.get("source");
      } else {
        hostName = decoder.getDefaultHostName();
      }
      // remove source/host from the tags list
      Map<String, String> wftags2 = new HashMap<>();
      for (Map.Entry<String, String> wftag : wftags.entrySet()) {
        if (wftag.getKey().equalsIgnoreCase("host") ||
            wftag.getKey().equalsIgnoreCase("source")) {
          continue;
        }
        wftags2.put(wftag.getKey(), wftag.getValue());
      }

      ReportPoint.Builder builder = ReportPoint.newBuilder();
      builder.setMetric(metricName);
      JsonNode time = metric.get("timestamp");
      long ts = Clock.now(); // if timestamp is not available, fall back to Clock.now()
      if (time != null) {
        int timestampSize = Long.toString(time.asLong()).length();
        if (timestampSize == 19) { // nanoseconds
          ts = time.asLong() / 1000000;
        } else if (timestampSize == 16) { // microseconds
          ts = time.asLong() / 1000;
        } else if (timestampSize == 13) { // milliseconds
          ts = time.asLong();
        } else { // seconds
          ts = time.asLong() * 1000;
        }
      }
      builder.setTimestamp(ts);
      JsonNode value = metric.get("value");
      if (value == null) {
        pointHandler.handleBlockedPoint("Skipping.  Missing 'value' in JSON node.");
        return false;
      }
      if (value.isDouble()) {
        builder.setValue(value.asDouble());
      } else {
        builder.setValue(value.asLong());
      }
      builder.setAnnotations(wftags2);
      builder.setTable("dummy");
      builder.setHost(hostName);
      ReportPoint point = builder.build();

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
      return true;
    } catch (final Exception e) {
      logWarning("WF-300: Failed to add metric", e, null);
      return false;
    }
  }
}

