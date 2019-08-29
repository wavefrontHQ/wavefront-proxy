package com.wavefront.agent.listeners;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.Clock;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportableEntityDecoder;
import com.wavefront.metrics.JsonMetricsParser;

import java.net.InetAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import wavefront.report.ReportPoint;

import static com.wavefront.agent.channel.CachingHostnameLookupResolver.getRemoteAddress;

/**
 * This class handles an incoming message of either String or FullHttpRequest type.  All other types are ignored. This
 * will likely be passed to the PlainTextOrHttpFrameDecoder as the handler for messages.
 *
 * @author Mike McLaughlin (mike@wavefront.com)
 */
public class OpenTSDBPortUnificationHandler extends PortUnificationHandler {
  private static final Logger logger = Logger.getLogger(
      OpenTSDBPortUnificationHandler.class.getCanonicalName());

  /**
   * The point handler that takes report metrics one data point at a time and handles batching and retries, etc
   */
  private final ReportableEntityHandler<ReportPoint> pointHandler;

  /**
   * OpenTSDB decoder object
   */
  private final ReportableEntityDecoder<String, ReportPoint> decoder;

  @Nullable
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;

  @Nullable
  private final Function<InetAddress, String> resolver;

  @SuppressWarnings("unchecked")
  public OpenTSDBPortUnificationHandler(
      final String handle, final TokenAuthenticator tokenAuthenticator,
      final HealthCheckManager healthCheckManager,
      final ReportableEntityDecoder<String, ReportPoint> decoder,
      final ReportableEntityHandlerFactory handlerFactory,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor,
      @Nullable final Function<InetAddress, String> resolver) {
    super(tokenAuthenticator, healthCheckManager, handle, true, true);
    this.decoder = decoder;
    this.pointHandler = handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT, handle));
    this.preprocessorSupplier = preprocessor;
    this.resolver = resolver;
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) {
    StringBuilder output = new StringBuilder();
    URI uri = parseUri(ctx, request);
    if (uri == null) return;

    switch (uri.getPath()) {
      case "/api/put":
        final ObjectMapper jsonTree = new ObjectMapper();
        HttpResponseStatus status;
        // from the docs:
        // The put endpoint will respond with a 204 HTTP status code and no content if all data points
        // were stored successfully. If one or more data points had an error, the API will return a 400.
        try {
          if (reportMetrics(jsonTree.readTree(request.content().toString(CharsetUtil.UTF_8)), ctx)) {
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
        writeHttpResponse(ctx, status, output, request);
        break;
      case "/api/version":
        // http://opentsdb.net/docs/build/html/api_http/version.html
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("version", ResourceBundle.getBundle("build").getString("build.version"));
        writeHttpResponse(ctx, HttpResponseStatus.OK, node, request);
        break;
      default:
        writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, "Unsupported path", request);
        logWarning("WF-300: Unexpected path '" + request.uri() + "'", null, ctx);
        break;
    }
  }

  /**
   * Handles an incoming plain text (string) message.
   */
  protected void handlePlainTextMessage(final ChannelHandlerContext ctx,
                                        String message) throws Exception {
    if (message == null) {
      throw new IllegalArgumentException("Message cannot be null");
    }
    if (tokenAuthenticator.authRequired()) { // plaintext is disabled with auth enabled
      pointHandler.reject(message, "Plaintext protocol disabled when authentication is enabled, ignoring");
      return;
    }
    if (message.startsWith("version")) {
      ChannelFuture f = ctx.writeAndFlush("Wavefront OpenTSDB Endpoint\n");
      if (!f.isSuccess()) {
        throw new Exception("Failed to write version response", f.cause());
      }
    } else {
      WavefrontPortUnificationHandler.preprocessAndHandlePoint(message, decoder, pointHandler,
          preprocessorSupplier, ctx);
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
   * @param metrics an array of objects or a single object representing a metric
   * @param ctx     channel handler context (to retrieve remote address)
   * @return true if all metrics added successfully; false o/w
   * @see #reportMetric(JsonNode, ChannelHandlerContext)
   */
  private boolean reportMetrics(final JsonNode metrics, ChannelHandlerContext ctx) {
    if (!metrics.isArray()) {
      return reportMetric(metrics, ctx);
    } else {
      boolean successful = true;
      for (final JsonNode metric : metrics) {
        if (!reportMetric(metric, ctx)) {
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
   * @param ctx    channel handler context (to retrieve remote address)
   * @return True if the metric was reported successfully; False o/w
   * @see <a href="http://opentsdb.net/docs/build/html/api_http/put.html">OpenTSDB /api/put documentation</a>
   */
  private boolean reportMetric(final JsonNode metric, ChannelHandlerContext ctx) {
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
        hostName = resolver == null ? "unknown" : resolver.apply(getRemoteAddress(ctx));
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
        pointHandler.reject((ReportPoint) null, "Skipping.  Missing 'value' in JSON node.");
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

      ReportableEntityPreprocessor preprocessor = preprocessorSupplier == null ? null : preprocessorSupplier.get();
      String[] messageHolder = new String[1];
      if (preprocessor != null) {
        preprocessor.forReportPoint().transform(point);
        if (!preprocessor.forReportPoint().filter(point, messageHolder)) {
          if (messageHolder[0] != null) {
            pointHandler.reject(point, messageHolder[0]);
            return false;
          } else {
            pointHandler.block(point);
            return true;
          }
        }
      }

      pointHandler.report(point);
      return true;
    } catch (final Exception e) {
      logWarning("WF-300: Failed to add metric", e, null);
      return false;
    }
  }
}
