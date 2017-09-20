package com.wavefront.agent;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.agent.preprocessor.PointPreprocessor;
import com.wavefront.common.Clock;
import com.wavefront.ingester.OpenTSDBDecoder;
import com.wavefront.metrics.JsonMetricsParser;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;
import wavefront.report.ReportPoint;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * This class handles an incoming message of either String or FullHttpRequest type.  All other types are ignored. This
 * will likely be passed to the PlainTextOrHttpFrameDecoder as the handler for messages.
 *
 * @author Mike McLaughlin (mike@wavefront.com)
 */
class OpenTSDBPortUnificationHandler extends SimpleChannelInboundHandler<Object> {
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
    this.decoder = decoder;
    this.pointHandler = pointHandler;
    this.preprocessor = preprocessor;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    blockMessage("WF-301", "Handler failed", cause, ctx);
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
          blockMessage("WF-300", "Unexpected message type " + message.getClass().getName(), null, ctx);
        }
      }
    } catch (final Exception e) {
      blockMessage("WF-300", "Failed to handle message", e, ctx);
    }
  }

  /**
   * Handles an incoming HTTP message.  The currently supported paths are:
   * {@link <ahref="http://opentsdb.net/docs/build/html/api_http/put.html">/api/put</a>}
   * {@link <ahref="http://opentsdb.net/docs/build/html/api_http/version.html">/api/version</a>},
   *
   * @throws IOException        when reading contents of HTTP body fails
   * @throws URISyntaxException when the request URI cannot be parsed
   */
  private void handleHttpMessage(final ChannelHandlerContext ctx,
                                 final Object message) {
    final FullHttpRequest request = (FullHttpRequest) message;
    URI uri;

    try{
      uri = new URI(request.uri());
    } catch (URISyntaxException e) {
      String errMsg = createErrMsg(e);
      writeHttpResponse(request, ctx, HttpResponseStatus.BAD_REQUEST, errMsg);
      blockMessage("WF-300", "Request URI, '" + request.uri() + "' cannot be parsed", e, ctx);
      return;
    }

    if (uri.getPath().equals("/api/put")) {
      final ObjectMapper jsonTree = new ObjectMapper();
      HttpResponseStatus status;
      String content = "";
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
          content = "At least one data point had error.";
        }
      } catch (Exception e) {
        status = HttpResponseStatus.BAD_REQUEST;
        if (e != null) {
          content = createErrMsg(e);
        }
        blockMessage("WF-300", "Failed to handle /api/put request", e, ctx);
      }
      writeHttpResponse(request, ctx, status, content);
    } else if (uri.getPath().equals("/api/version")) {
      writeHttpResponse(request, ctx, HttpResponseStatus.OK,
          "Wavefront OpenTSDB Endpoint"); // TODO: should be a JSON response object (see docs)
      // http://opentsdb.net/docs/build/html/api_http/version.html
    } else {
      writeHttpResponse(request, ctx, HttpResponseStatus.BAD_REQUEST, "Unsupported path");
      blockMessage("WF-300", "Unexpected path '" + request.uri() + "'", null, ctx);
    }
  }

  /**
   * Handles an incoming plain text (string) message. Handles :
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
    if (messageStr.length() > 7 && messageStr.substring(0, 7).equals("version")) {
      ChannelFuture f = ctx.writeAndFlush("Wavefront OpenTSDB Endpoint\n");
      if (!f.isSuccess()) {
        throw new Exception("Failed to write version response", f.cause());
      }
    } else {
      ChannelStringHandler.processPointLine(messageStr, decoder, pointHandler, preprocessor, ctx);
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
      blockMessage("WF-300", "Failed to add metric", e, null);
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
    boolean keepAlive = HttpUtil.isKeepAlive(request);
    // Build the response object.
    FullHttpResponse response = new DefaultFullHttpResponse(
        HTTP_1_1, status, Unpooled.copiedBuffer(contents, CharsetUtil.UTF_8));
    response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");

    if (keepAlive) {
      // Add 'Content-Length' header only for a keep-alive connection.
      response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
      // Add keep alive header as per:
      // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
      response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
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
   * @param ctx       ChannelHandlerContext (optional) to extract remote client ip
   */
  private void blockMessage(final String messageId,
                            final String message,
                            @Nullable final Throwable e,
                            @Nullable final ChannelHandlerContext ctx) {
    String errMsg = messageId + ": OpenTSDB: " + message;
    if (ctx != null) {
      InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
      if (remoteAddress != null) {
        errMsg += "; remote: " + remoteAddress.getHostString();
      }
    }
    if (e != null) {
      final Throwable rootCause = Throwables.getRootCause(e);
      errMsg = errMsg + "; " + createErrMsg(e);
      logger.log(Level.WARNING, errMsg, e);
    } else {
      logger.warning(errMsg);
    }

    pointHandler.handleBlockedPoint(errMsg);
  }

  /**
   * Create a error message from an exception.
   *
   * @param e Exceptions thrown
   * @return formatted error message
   */
  private String createErrMsg(@Nonnull final Throwable e) {
    final Throwable rootCause = Throwables.getRootCause(e);
    String errMsg = "reason: \"" + e.getMessage() + "\"";
    if (rootCause != null && rootCause.getMessage() != null) {
      errMsg = errMsg + ", root cause: \"" + rootCause.getMessage() + "\"";
    }

    return errMsg;
  }
}

