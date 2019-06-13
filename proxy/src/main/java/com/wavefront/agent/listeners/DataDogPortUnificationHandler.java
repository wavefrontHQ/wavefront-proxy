package com.wavefront.agent.listeners;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.wavefront.agent.auth.TokenAuthenticatorBuilder;
import com.wavefront.agent.auth.TokenValidationMethod;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.Clock;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportPointSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import wavefront.report.ReportPoint;

import static io.netty.handler.codec.http.HttpMethod.POST;

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
  private final ReportableEntityHandler<ReportPoint> pointHandler;
  private final boolean processSystemMetrics;
  private final boolean processServiceChecks;
  @Nullable
  private final HttpClient requestRelayClient;
  @Nullable
  private final String requestRelayTarget;

  @Nullable
  private final ReportableEntityPreprocessor preprocessor;

  private final ObjectMapper jsonParser;

  private final Cache<String, Map<String, String>> tagsCache = Caffeine.newBuilder().
      expireAfterWrite(6, TimeUnit.HOURS).
      maximumSize(100_000).
      build();

  @SuppressWarnings("unchecked")
  public DataDogPortUnificationHandler(final String handle,
                                       final ReportableEntityHandlerFactory handlerFactory,
                                       final boolean processSystemMetrics,
                                       final boolean processServiceChecks,
                                       @Nullable final HttpClient requestRelayClient,
                                       @Nullable final String requestRelayTarget,
                                       @Nullable final ReportableEntityPreprocessor preprocessor) {
    this(handle, handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT, handle)), processSystemMetrics,
        processServiceChecks, requestRelayClient, requestRelayTarget, preprocessor);
  }


  @VisibleForTesting
  protected DataDogPortUnificationHandler(final String handle,
                                          final ReportableEntityHandler<ReportPoint> pointHandler,
                                          final boolean processSystemMetrics,
                                          final boolean processServiceChecks,
                                          @Nullable final HttpClient requestRelayClient,
                                          @Nullable final String requestRelayTarget,
                                          @Nullable final ReportableEntityPreprocessor preprocessor) {
    super(TokenAuthenticatorBuilder.create().setTokenValidationMethod(TokenValidationMethod.NONE).build(), handle,
        false, true);
    this.pointHandler = pointHandler;
    this.processSystemMetrics = processSystemMetrics;
    this.processServiceChecks = processServiceChecks;
    this.requestRelayClient = requestRelayClient;
    this.requestRelayTarget = requestRelayTarget;
    this.preprocessor = preprocessor;
    this.jsonParser = new ObjectMapper();
    this.httpRequestSize = Metrics.newHistogram(new TaggedMetricName("listeners", "http-requests.payload-points",
        "port", handle));

    Metrics.newGauge(new TaggedMetricName("listeners", "tags-cache-size",
        "port", handle), new Gauge<Long>() {
      @Override
      public Long value() {
        return tagsCache.estimatedSize();
      }
    });

  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest incomingRequest) {
    StringBuilder output = new StringBuilder();
    AtomicInteger pointsPerRequest = new AtomicInteger();

    URI uri = parseUri(ctx, incomingRequest);
    if (uri == null) return;

    HttpResponseStatus status = HttpResponseStatus.ACCEPTED;
    String requestBody = incomingRequest.content().toString(CharsetUtil.UTF_8);

    if (requestRelayClient != null && requestRelayTarget != null && incomingRequest.method() == POST) {
      Histogram requestRelayDuration = Metrics.newHistogram(new TaggedMetricName("listeners",
          "http-relay.duration-nanos", "port", handle));
      Long startNanos = System.nanoTime();
      try {
        String outgoingUrl = requestRelayTarget.replaceFirst("/*$", "") + incomingRequest.uri();
        HttpPost outgoingRequest = new HttpPost(outgoingUrl);
        if (incomingRequest.headers().contains("Content-Type")) {
          outgoingRequest.addHeader("Content-Type", incomingRequest.headers().get("Content-Type"));
        }
        outgoingRequest.setEntity(new StringEntity(requestBody));
        logger.info("Relaying incoming HTTP request to " + outgoingUrl);
        HttpResponse response = requestRelayClient.execute(outgoingRequest);
        int httpStatusCode = response.getStatusLine().getStatusCode();
        Metrics.newCounter(new TaggedMetricName("listeners", "http-relay.status." + httpStatusCode + ".count",
            "port", handle)).inc();

        if (httpStatusCode < 200 || httpStatusCode >= 300) {
          // anything that is not 2xx is relayed as is to the client, don't process the payload
          writeHttpResponse(ctx, HttpResponseStatus.valueOf(httpStatusCode),
              EntityUtils.toString(response.getEntity(), "UTF-8"), incomingRequest);
          return;
        }
        EntityUtils.consumeQuietly(response.getEntity());

      } catch (IOException e) {
        logger.warning("Unable to relay request to " + requestRelayTarget + ": " + e.getMessage());
        Metrics.newCounter(new TaggedMetricName("listeners", "http-relay.failed",
            "port", handle)).inc();
        writeHttpResponse(ctx, HttpResponseStatus.BAD_GATEWAY, "Unable to relay request: " + e.getMessage(),
            incomingRequest);
        return;
      } finally {
        requestRelayDuration.update(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
      }
    }

    String path = uri.getPath().endsWith("/") ? uri.getPath() : uri.getPath() + "/";
    switch (path) {
      case "/api/v1/series/":
        try {
          if (!reportMetrics(jsonParser.readTree(requestBody), pointsPerRequest)) {
            status = HttpResponseStatus.BAD_REQUEST;
            output.append("At least one data point had error.");
          }
        } catch (Exception e) {
          status = HttpResponseStatus.BAD_REQUEST;
          writeExceptionText(e, output);
          logWarning("WF-300: Failed to handle /api/v1/series request", e, ctx);
        }
        httpRequestSize.update(pointsPerRequest.intValue());
        writeHttpResponse(ctx, status, output, incomingRequest);
        break;

      case "/api/v1/check_run/":
        if (!processServiceChecks) {
          Metrics.newCounter(new TaggedMetricName("listeners", "http-requests.ignored", "port", handle)).inc();
          writeHttpResponse(ctx, HttpResponseStatus.ACCEPTED, output, incomingRequest);
          return;
        }
        try {
          if (!reportChecks(jsonParser.readTree(requestBody), pointsPerRequest)) {
            output.append("One or more checks were not valid.");
          }
        } catch (Exception e) {
          status = HttpResponseStatus.BAD_REQUEST;
          writeExceptionText(e, output);
          logWarning("WF-300: Failed to handle /api/v1/check_run request", e, ctx);
        }
        writeHttpResponse(ctx, status, output, incomingRequest);
        break;

      case "/api/v1/validate/":
        writeHttpResponse(ctx, HttpResponseStatus.OK, output, incomingRequest);
        break;

      case "/intake/":
        if (!processSystemMetrics) {
          Metrics.newCounter(new TaggedMetricName("listeners", "http-requests.ignored", "port", handle)).inc();
          writeHttpResponse(ctx, HttpResponseStatus.ACCEPTED, output, incomingRequest);
          return;
        }
        try {
          if (!reportSystemMetrics(jsonParser.readTree(requestBody), pointsPerRequest)) {
            output.append("At least one data point had error.");
          }
        } catch (Exception e) {
          status = HttpResponseStatus.BAD_REQUEST;
          writeExceptionText(e, output);
          logWarning("WF-300: Failed to handle /intake request", e, ctx);
        }
        httpRequestSize.update(pointsPerRequest.intValue());
        writeHttpResponse(ctx, status, output, incomingRequest);
        break;

      default:
        writeHttpResponse(ctx, HttpResponseStatus.NO_CONTENT, output, incomingRequest);
        logWarning("WF-300: Unexpected path '" + incomingRequest.uri() + "', returning HTTP 204", null, ctx);
        break;
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
    if (metrics == null || !metrics.isObject() || !metrics.has("series")) {
      pointHandler.reject((ReportPoint) null, "WF-300: Payload missing 'series' field");
      return false;
    }
    JsonNode series = metrics.get("series");
    if (!series.isArray()) {
      pointHandler.reject((ReportPoint) null, "WF-300: 'series' field must be an array");
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
      pointHandler.reject((ReportPoint) null, "Skipping - series object null.");
      return false;
    }
    try {
      if (metric.get("metric") == null ) {
        pointHandler.reject((ReportPoint) null, "Skipping - 'metric' field missing.");
        return false;
      }
      String metricName = INVALID_METRIC_CHARACTERS.matcher(metric.get("metric").textValue()).replaceAll("_");
      String hostName = metric.get("host") == null ? "unknown" : metric.get("host").textValue().toLowerCase();
      JsonNode tagsNode = metric.get("tags");
      Map<String, String> systemTags;
      Map<String, String> tags = new HashMap<>();
      if ((systemTags = tagsCache.getIfPresent(hostName)) != null) {
        tags.putAll(systemTags);
      }
      extractTags(tagsNode, tags); // tags sent with the data override system host-level tags
      JsonNode pointsNode = metric.get("points");
      if (pointsNode == null) {
        pointHandler.reject((ReportPoint) null, "Skipping - 'points' field missing.");
        return false;
      }
      for (JsonNode node : pointsNode) {
        if (node.size() == 2) {
          reportValue(metricName, hostName, tags, node.get(1), node.get(0).longValue() * 1000, pointCounter);
        } else {
          pointHandler.reject((ReportPoint) null, "WF-300: Inconsistent point value size (expected: 2)");
        }
      }
      return true;
    } catch (final Exception e) {
      logger.log(Level.WARNING, "WF-300: Failed to add metric", e);
      return false;
    }
  }

  private boolean reportChecks(final JsonNode checkNode, @Nullable final AtomicInteger pointCounter) {
    if (checkNode == null) {
      pointHandler.reject((ReportPoint) null, "Skipping - check object is null.");
      return false;
    }
    if (checkNode.isArray()) {
      boolean result = true;
      for (JsonNode check : checkNode) {
        result &= reportCheck(check, pointCounter);
      }
      return result;
    } else {
      return reportCheck(checkNode, pointCounter);
    }
  }

  private boolean reportCheck(final JsonNode check, @Nullable final AtomicInteger pointCounter) {
    try {
      if (check.get("check") == null ) {
        pointHandler.reject((ReportPoint) null, "Skipping - 'check' field missing.");
        return false;
      }
      if (check.get("host_name") == null ) {
        pointHandler.reject((ReportPoint) null, "Skipping - 'host_name' field missing.");
        return false;
      }
      if (check.get("status") == null ) {
        // ignore - there is no status to update
        return true;
      }
      String metricName = INVALID_METRIC_CHARACTERS.matcher(check.get("check").textValue()).replaceAll("_");
      String hostName = check.get("host_name").textValue().toLowerCase();
      JsonNode tagsNode = check.get("tags");
      Map<String, String> systemTags;
      Map<String, String> tags = new HashMap<>();
      if ((systemTags = tagsCache.getIfPresent(hostName)) != null) {
        tags.putAll(systemTags);
      }
      extractTags(tagsNode, tags); // tags sent with the data override system host-level tags

      long timestamp = check.get("timestamp") == null ? Clock.now() : check.get("timestamp").asLong() * 1000;
      reportValue(metricName, hostName, tags, check.get("status"), timestamp, pointCounter);
      return true;
    } catch (final Exception e) {
      logger.log(Level.WARNING, "WF-300: Failed to add metric", e);
      return false;
    }
  }


  private boolean reportSystemMetrics(final JsonNode metrics, @Nullable final AtomicInteger pointCounter) {
    if (metrics == null || !metrics.isObject() || !metrics.has("collection_timestamp")) {
      pointHandler.reject((ReportPoint) null, "WF-300: Payload missing 'collection_timestamp' field");
      return false;
    }
    long timestamp = metrics.get("collection_timestamp").asLong() * 1000;
    if (!metrics.has("internalHostname")) {
      pointHandler.reject((ReportPoint) null, "WF-300: Payload missing 'internalHostname' field");
      return false;
    }
    String hostName = metrics.get("internalHostname").textValue().toLowerCase();
    Map<String, String> systemTags = new HashMap<>();
    if (metrics.has("host-tags") && metrics.get("host-tags").get("system") != null) {
      extractTags(metrics.get("host-tags").get("system"), systemTags);
      tagsCache.put(hostName, systemTags); // cache even if map is empty so we know how many unique hosts report metrics.
    } else {
      Map<String, String> cachedTags = tagsCache.getIfPresent(hostName);
      if (cachedTags != null) {
        systemTags.clear();
        systemTags.putAll(cachedTags);
      }
    }

    // Report "system.io." metrics
    JsonNode ioStats = metrics.get("ioStats");
    if (ioStats != null && ioStats.isObject()) {
      ioStats.fields().forEachRemaining(entry -> {
        Map<String, String> deviceTags = ImmutableMap.<String, String>builder().
            putAll(systemTags).
            put("device", entry.getKey()).
            build();
        if (entry.getValue() != null && entry.getValue().isObject()) {
          entry.getValue().fields().forEachRemaining(metricEntry -> {
            String metric = "system.io." + metricEntry.getKey().replace('%', ' ').replace('/', '_').trim();
            reportValue(metric, hostName, deviceTags, metricEntry.getValue(), timestamp, pointCounter);
          });
        }
      });
    }

    // Report all metrics that already start with "system."
    metrics.fields().forEachRemaining(entry -> {
      if (entry.getKey().startsWith("system.")) {
        reportValue(entry.getKey(), hostName, systemTags, entry.getValue(), timestamp, pointCounter);
      }
    });

    // Report CPU and memory metrics
    reportValue("system.cpu.guest", hostName, systemTags, metrics.get("cpuGuest"), timestamp, pointCounter);
    reportValue("system.cpu.idle", hostName, systemTags, metrics.get("cpuIdle"), timestamp, pointCounter);
    reportValue("system.cpu.stolen", hostName, systemTags, metrics.get("cpuStolen"), timestamp, pointCounter);
    reportValue("system.cpu.system", hostName, systemTags, metrics.get("cpuSystem"), timestamp, pointCounter);
    reportValue("system.cpu.user", hostName, systemTags, metrics.get("cpuUser"), timestamp, pointCounter);
    reportValue("system.cpu.wait", hostName, systemTags, metrics.get("cpuWait"), timestamp, pointCounter);
    reportValue("system.mem.buffers", hostName, systemTags, metrics.get("memBuffers"), timestamp, pointCounter);
    reportValue("system.mem.cached", hostName, systemTags, metrics.get("memCached"), timestamp, pointCounter);
    reportValue("system.mem.page_tables", hostName, systemTags, metrics.get("memPageTables"), timestamp, pointCounter);
    reportValue("system.mem.shared", hostName, systemTags, metrics.get("memShared"), timestamp, pointCounter);
    reportValue("system.mem.slab", hostName, systemTags, metrics.get("memSlab"), timestamp, pointCounter);
    reportValue("system.mem.free", hostName, systemTags, metrics.get("memPhysFree"), timestamp, pointCounter);
    reportValue("system.mem.pct_usable", hostName, systemTags, metrics.get("memPhysPctUsable"), timestamp, pointCounter);
    reportValue("system.mem.total", hostName, systemTags, metrics.get("memPhysTotal"), timestamp, pointCounter);
    reportValue("system.mem.usable", hostName, systemTags, metrics.get("memPhysUsable"), timestamp, pointCounter);
    reportValue("system.mem.used", hostName, systemTags, metrics.get("memPhysUsed"), timestamp, pointCounter);
    reportValue("system.swap.cached", hostName, systemTags, metrics.get("memSwapCached"), timestamp, pointCounter);
    reportValue("system.swap.free", hostName, systemTags, metrics.get("memSwapFree"), timestamp, pointCounter);
    reportValue("system.swap.pct_free", hostName, systemTags, metrics.get("memSwapPctFree"), timestamp, pointCounter);
    reportValue("system.swap.total", hostName, systemTags, metrics.get("memSwapTotal"), timestamp, pointCounter);
    reportValue("system.swap.used", hostName, systemTags, metrics.get("memSwapUsed"), timestamp, pointCounter);
    return true;
  }

  private void reportValue(String metricName, String hostName, Map<String, String> tags, JsonNode valueNode,
                              long timestamp, AtomicInteger pointCounter) {
    if (valueNode == null || valueNode.isNull()) return;
    double value;
    if (valueNode.isTextual()) {
      try {
        value = Double.parseDouble(valueNode.textValue());
      } catch (NumberFormatException nfe) {
        return;
      }
    } else if (valueNode.isBoolean()) {
      value = valueNode.asBoolean() ? 1.0d : 0.0d;
    } else if (valueNode.isDouble()) {
      value = valueNode.asDouble();
    } else {
      value = valueNode.asLong();
    }

    ReportPoint point = ReportPoint.newBuilder().
        setTable("dummy").
        setMetric(metricName).
        setHost(hostName).
        setTimestamp(timestamp).
        setAnnotations(tags).
        setValue(value).
        build();
    if (pointCounter != null) {
      pointCounter.incrementAndGet();
    }
    if (preprocessor != null) {
      preprocessor.forReportPoint().transform(point);
      if (!preprocessor.forReportPoint().filter(point)) {
        if (preprocessor.forReportPoint().getLastFilterResult() != null) {
          blockedPointsLogger.warning(ReportPointSerializer.pointToString(point));
        } else {
          blockedPointsLogger.info(ReportPointSerializer.pointToString(point));
        }
        pointHandler.reject(point, preprocessor.forReportPoint().getLastFilterResult());
        return;
      }
    }
    pointHandler.report(point);
  }

  private void extractTags(JsonNode tagsNode, final Map<String, String> tags) {
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
