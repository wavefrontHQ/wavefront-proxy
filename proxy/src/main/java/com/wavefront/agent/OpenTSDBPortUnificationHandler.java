package com.wavefront.agent;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.agent.preprocessor.PointPreprocessor;
import com.wavefront.ingester.OpenTSDBDecoder;
import com.wavefront.metrics.JsonMetricsParser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import sunnylabs.report.ReportPoint;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * This class handles an incoming message of either String or FullHttpRequest type.  All other types
 * are ignored. This will likely be passed to the PlainTextOrHttpFrameDecoder as the handler for
 * messages.
 * @author Mike McLaughlin (mike@wavefront.com)
 */
class OpenTSDBPortUnificationHandler extends SimpleChannelInboundHandler<Object> {
  private static final Logger logger = Logger.getLogger(
      OpenTSDBPortUnificationHandler.class.getCanonicalName());

  /**
   * The point handler that takes report metrics one data point at a time and handles batching and
   * retries, etc
   */
  private final PointHandlerImpl pointHandler;

  /**
   * OpenTSDB decoder object
   */
  private final OpenTSDBDecoder decoder;

  @Nullable
  private final PointPreprocessor<String> pointLinePreprocessor;
  @Nullable
  private final PointPreprocessor<ReportPoint> reportPointPreprocessor;

  OpenTSDBPortUnificationHandler(final OpenTSDBDecoder decoder,
                                 final int port,
                                 final String validationLevel,
                                 final int blockedPointsPerBatch,
                                 final PostPushDataTimedTask[] postPushDataTimedTasks,
                                 @Nullable final PointPreprocessor<String> pointLinePreprocessor,
                                 @Nullable final PointPreprocessor<ReportPoint> reportPointPreprocessor) {
    this.decoder = decoder;
    this.pointHandler = new PointHandlerImpl(
        port, validationLevel, blockedPointsPerBatch, postPushDataTimedTasks);
    this.pointLinePreprocessor = pointLinePreprocessor;
    this.reportPointPreprocessor = reportPointPreprocessor;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    blockMessage("WF-301", "Handler failed", cause);
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx,
                              final Object message) {
    try {
      if (message != null) {
        if (message instanceof String) {
          handlePlainTextMessage(ctx, message);
        } else if (message instanceof FullHttpRequest) {
          handleHttpMessage(ctx, message);
        } else {
          blockMessage("WF-300", "Unexpected message type " + message.getClass().getName(), null);
        }
      }
    } catch (final Exception e) {
      blockMessage("WF-300", "Failed to handle message", e);
    }
  }

  /**
   * Handles an incoming HTTP message.  The currently supported paths are:
   *   {@link <a href="http://opentsdb.net/docs/build/html/api_http/put.html">/api/put</a>}
   *   {@link <a href="http://opentsdb.net/docs/build/html/api_http/version.html">/api/version</a>},
   *
   * @throws IOException        when reading contents of HTTP body fails
   * @throws URISyntaxException when the request URI cannot be parsed
   */
  private void handleHttpMessage(final ChannelHandlerContext ctx,
                                 final Object message) throws IOException, URISyntaxException {
    final FullHttpRequest request = (FullHttpRequest) message;
    final URI uri = new URI(request.getUri());
    if (uri.getPath().equals("/api/put")) {
      final ObjectMapper jsonTree = new ObjectMapper();
      HttpResponseStatus status;
      // from the docs:
      // The put endpoint will respond with a 204 HTTP status code and no content if all data points
      // were stored successfully. If one or more data points had an error, the API will return a 400.
      if (reportMetrics(jsonTree.readTree(request.content().toString(CharsetUtil.UTF_8)))) {
        status = HttpResponseStatus.NO_CONTENT;
      } else {
        status = HttpResponseStatus.BAD_REQUEST;
      }
      writeHttpResponse(request, ctx, status, "");
    } else if (uri.getPath().equals("/api/version")) {
      writeHttpResponse(request, ctx, HttpResponseStatus.OK,
          "Wavefront OpenTSDB Endpoint"); // TODO: should be a JSON response object (see docs)
      // http://opentsdb.net/docs/build/html/api_http/version.html
    } else {
      writeHttpResponse(request, ctx, HttpResponseStatus.BAD_REQUEST, "Unsupported path");
      blockMessage("WF-300", "Unexpected path '" + request.getUri() + "'", null);
    }
  }

  /**
   * Handles an incoming plain text (string) message.
   * Handles :
   *   
   */
  private void handlePlainTextMessage(final ChannelHandlerContext ctx,
                                      final Object message) throws Exception {
    final String messageStr = (String) message;
    if (message == null) {
      throw new IllegalArgumentException("Message cannot be null");
    }
    if (logger.isLoggable(Level.FINE)) {
      logger.fine("Handling plain text message: " + messageStr);
    }
    if (messageStr.substring(0, 7).equals("version")) {
      ChannelFuture f = ctx.writeAndFlush("Wavefront OpenTSDB Endpoint\n");
      if (!f.isSuccess()) {
        throw new Exception("Failed to write version response", f.cause());
      }
    } else {
      ChannelStringHandler.processPointLine(messageStr, decoder, pointHandler,
                                            pointLinePreprocessor, reportPointPreprocessor);
    }
  }

  /**
   * Parse the metrics JSON and report the metrics found.  There are 2 formats supported: - array of
   * points - single point
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
    final PostPushDataTimedTask randomPostTask = this.pointHandler.getRandomPostTask();
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
      long ts = 0;
      if (time != null) {
        if (Long.toString(ts).length() == 10) {
          ts = time.asLong() * 1000;
        } else {
          ts = time.asLong();
        }
      }
      builder.setTimestamp(ts);
      JsonNode value = metric.get("value");
      if (value == null) {
        randomPostTask.incrementBlockedPoints();
        logger.warning("Skipping.  Missing 'value' in JSON node.");
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
      pointHandler.reportPoint(point, "OpenTSDB http json: " + PointHandlerImpl.pointToString(point));
      return true;
    } catch (final Exception e) {
      blockMessage("WF-300", "Failed to add metric", e);
      return false;
    }
  }

  /**
   * Writes an HTTP response
   */
  private void writeHttpResponse(HttpRequest request,
                                 ChannelHandlerContext ctx,
                                 HttpResponseStatus status,
                                 String contents) {
    // Decide whether to close the connection or not.
    boolean keepAlive = HttpHeaders.isKeepAlive(request);
    // Build the response object.
    FullHttpResponse response = new DefaultFullHttpResponse(
        HTTP_1_1, status, Unpooled.copiedBuffer(contents, CharsetUtil.UTF_8));
    response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");

    if (keepAlive) {
      // Add 'Content-Length' header only for a keep-alive connection.
      response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
      // Add keep alive header as per:
      // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
      response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    }

    // Write the response.
    ctx.write(response);

    if (!keepAlive) {
      ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
          .addListener(ChannelFutureListener.CLOSE);
    }
  }

  /**
   * Logs and handles a blocked metric
   *
   * @param messageId the error message ID
   * @param message   the error message
   * @param e         the exception (optional) that caused the message to be blocked
   */
  private void blockMessage(final String messageId, final String message, @Nullable final Throwable e) {
    String errMsg = messageId + ": OpenTSDB: " + message;
    if (e != null) {
      final Throwable rootCause = Throwables.getRootCause(e);
      errMsg = errMsg + "; reason: \"" + e.getMessage() + "\"";
      if (rootCause != null && rootCause.getMessage() != null) {
        errMsg = errMsg + ", root cause: \"" + rootCause.getMessage() + "\"";
      }
      logger.log(Level.WARNING, errMsg, e);
    } else {
      logger.warning(errMsg);
    }

    pointHandler.handleBlockedPoint(errMsg);
  }
}

