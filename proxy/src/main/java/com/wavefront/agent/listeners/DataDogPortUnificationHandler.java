package com.wavefront.agent.listeners;

import static com.wavefront.agent.ProxyContext.queuesManager;
import static com.wavefront.agent.channel.ChannelUtils.errorMessageWithRootCause;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static io.netty.handler.codec.http.HttpMethod.POST;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.auth.TokenAuthenticatorBuilder;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.core.handlers.ReportableEntityHandler;
import com.wavefront.agent.core.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.Clock;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportPointSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import wavefront.report.ReportPoint;

/**
 * Accepts incoming HTTP requests in DataDog JSON format. has the ability to relay them to DataDog.
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class DataDogPortUnificationHandler extends AbstractHttpOnlyHandler {
  private static final Logger logger =
      Logger.getLogger(DataDogPortUnificationHandler.class.getCanonicalName());
  private static final Logger blockedPointsLogger = Logger.getLogger("RawBlockedPoints");
  private static final Pattern INVALID_METRIC_CHARACTERS = Pattern.compile("[^-_\\.\\dA-Za-z]");
  private static final Pattern INVALID_TAG_CHARACTERS = Pattern.compile("[^-_:\\.\\\\/\\dA-Za-z]");

  private static final Map<String, String> SYSTEM_METRICS =
      ImmutableMap.<String, String>builder()
          .put("system.cpu.guest", "cpuGuest")
          .put("system.cpu.idle", "cpuIdle")
          .put("system.cpu.stolen", "cpuStolen")
          .put("system.cpu.system", "cpuSystem")
          .put("system.cpu.user", "cpuUser")
          .put("system.cpu.wait", "cpuWait")
          .put("system.mem.buffers", "memBuffers")
          .put("system.mem.cached", "memCached")
          .put("system.mem.page_tables", "memPageTables")
          .put("system.mem.shared", "memShared")
          .put("system.mem.slab", "memSlab")
          .put("system.mem.free", "memPhysFree")
          .put("system.mem.pct_usable", "memPhysPctUsable")
          .put("system.mem.total", "memPhysTotal")
          .put("system.mem.usable", "memPhysUsable")
          .put("system.mem.used", "memPhysUsed")
          .put("system.swap.cached", "memSwapCached")
          .put("system.swap.free", "memSwapFree")
          .put("system.swap.pct_free", "memSwapPctFree")
          .put("system.swap.total", "memSwapTotal")
          .put("system.swap.used", "memSwapUsed")
          .build();

  private final Histogram httpRequestSize;

  /**
   * The point handler that takes report metrics one data point at a time and handles batching and
   * retries, etc
   */
  private final ReportableEntityHandler<ReportPoint> pointHandler;

  private final boolean synchronousMode;
  private final boolean processSystemMetrics;
  private final boolean processServiceChecks;
  @Nullable private final HttpClient requestRelayClient;
  @Nullable private final String requestRelayTarget;

  @Nullable private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;

  private final ObjectMapper jsonParser;

  private final Cache<String, Map<String, String>> tagsCache =
      Caffeine.newBuilder().expireAfterWrite(6, TimeUnit.HOURS).maximumSize(100_000).build();
  private final LoadingCache<Integer, Counter> httpStatusCounterCache;
  private final ScheduledThreadPoolExecutor threadpool;

  public DataDogPortUnificationHandler(
      final int port,
      final HealthCheckManager healthCheckManager,
      final ReportableEntityHandlerFactory handlerFactory,
      final int fanout,
      final boolean synchronousMode,
      final boolean processSystemMetrics,
      final boolean processServiceChecks,
      @Nullable final HttpClient requestRelayClient,
      @Nullable final String requestRelayTarget,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor) {
    this(
        port,
        healthCheckManager,
        handlerFactory.getHandler(port, queuesManager.initQueue(ReportableEntityType.POINT)),
        fanout,
        synchronousMode,
        processSystemMetrics,
        processServiceChecks,
        requestRelayClient,
        requestRelayTarget,
        preprocessor);
  }

  @VisibleForTesting
  protected DataDogPortUnificationHandler(
      final int port,
      final HealthCheckManager healthCheckManager,
      final ReportableEntityHandler<ReportPoint> pointHandler,
      final int fanout,
      final boolean synchronousMode,
      final boolean processSystemMetrics,
      final boolean processServiceChecks,
      @Nullable final HttpClient requestRelayClient,
      @Nullable final String requestRelayTarget,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessor) {
    super(TokenAuthenticatorBuilder.create().build(), healthCheckManager, port);
    this.pointHandler = pointHandler;
    this.threadpool = new ScheduledThreadPoolExecutor(fanout, new NamedThreadFactory("dd-relay"));
    this.synchronousMode = synchronousMode;
    this.processSystemMetrics = processSystemMetrics;
    this.processServiceChecks = processServiceChecks;
    this.requestRelayClient = requestRelayClient;
    this.requestRelayTarget = requestRelayTarget;
    this.preprocessorSupplier = preprocessor;
    this.jsonParser = new ObjectMapper();
    this.httpRequestSize =
        Metrics.newHistogram(
            new TaggedMetricName(
                "listeners", "http-requests.payload-points", "port", String.valueOf(port)));
    this.httpStatusCounterCache =
        Caffeine.newBuilder()
            .build(
                status ->
                    Metrics.newCounter(
                        new TaggedMetricName(
                            "listeners",
                            "http-relay.status." + status + ".count",
                            "port",
                            String.valueOf(port))));
    Metrics.newGauge(
        new TaggedMetricName("listeners", "tags-cache-size", "port", String.valueOf(port)),
        new Gauge<Long>() {
          @Override
          public Long value() {
            return tagsCache.estimatedSize();
          }
        });
    Metrics.newGauge(
        new TaggedMetricName(
            "listeners", "http-relay.threadpool.queue-size", "port", String.valueOf(port)),
        new Gauge<Integer>() {
          @Override
          public Integer value() {
            return threadpool.getQueue().size();
          }
        });
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx, final FullHttpRequest request)
      throws URISyntaxException {
    StringBuilder output = new StringBuilder();
    AtomicInteger pointsPerRequest = new AtomicInteger();
    URI uri = new URI(request.uri());
    HttpResponseStatus status = HttpResponseStatus.ACCEPTED;
    String requestBody = request.content().toString(CharsetUtil.UTF_8);

    if (requestRelayClient != null && requestRelayTarget != null && request.method() == POST) {
      Histogram requestRelayDuration =
          Metrics.newHistogram(
              new TaggedMetricName(
                  "listeners", "http-relay.duration-nanos", "port", String.valueOf(port)));
      long startNanos = System.nanoTime();
      try {
        String outgoingUrl = requestRelayTarget.replaceFirst("/*$", "") + request.uri();
        HttpPost outgoingRequest = new HttpPost(outgoingUrl);
        if (request.headers().contains("Content-Type")) {
          outgoingRequest.addHeader("Content-Type", request.headers().get("Content-Type"));
        }
        outgoingRequest.setEntity(new StringEntity(requestBody));
        if (synchronousMode) {
          if (logger.isLoggable(Level.FINE)) {
            logger.fine("Relaying incoming HTTP request to " + outgoingUrl);
          }
          HttpResponse response = requestRelayClient.execute(outgoingRequest);
          int httpStatusCode = response.getStatusLine().getStatusCode();
          httpStatusCounterCache.get(httpStatusCode).inc();

          if (httpStatusCode < 200 || httpStatusCode >= 300) {
            // anything that is not 2xx is relayed as is to the client, don't process the payload
            writeHttpResponse(
                ctx,
                HttpResponseStatus.valueOf(httpStatusCode),
                EntityUtils.toString(response.getEntity(), "UTF-8"),
                request);
            return;
          }
        } else {
          threadpool.submit(
              () -> {
                try {
                  if (logger.isLoggable(Level.FINE)) {
                    logger.fine("Relaying incoming HTTP request (async) to " + outgoingUrl);
                  }
                  HttpResponse response = requestRelayClient.execute(outgoingRequest);
                  int httpStatusCode = response.getStatusLine().getStatusCode();
                  httpStatusCounterCache.get(httpStatusCode).inc();
                  EntityUtils.consumeQuietly(response.getEntity());
                } catch (IOException e) {
                  logger.warning(
                      "Unable to relay request to " + requestRelayTarget + ": " + e.getMessage());
                  Metrics.newCounter(
                          new TaggedMetricName(
                              "listeners", "http-relay.failed", "port", String.valueOf(port)))
                      .inc();
                }
              });
        }
      } catch (IOException e) {
        logger.warning("Unable to relay request to " + requestRelayTarget + ": " + e.getMessage());
        Metrics.newCounter(
                new TaggedMetricName(
                    "listeners", "http-relay.failed", "port", String.valueOf(port)))
            .inc();
        writeHttpResponse(
            ctx,
            HttpResponseStatus.BAD_GATEWAY,
            "Unable to relay request: " + e.getMessage(),
            request);
        return;
      } finally {
        requestRelayDuration.update(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
      }
    }

    String path = uri.getPath().endsWith("/") ? uri.getPath() : uri.getPath() + "/";
    switch (path) {
      case "/api/v1/series/":
        try {
          status =
              reportMetrics(jsonParser.readTree(requestBody), pointsPerRequest, output::append);
        } catch (Exception e) {
          status = HttpResponseStatus.BAD_REQUEST;
          output.append(errorMessageWithRootCause(e));
          logWarning("WF-300: Failed to handle /api/v1/series request", e, ctx);
        }
        httpRequestSize.update(pointsPerRequest.intValue());
        writeHttpResponse(ctx, status, output, request);
        break;

      case "/api/v1/check_run/":
        if (!processServiceChecks) {
          Metrics.newCounter(
                  new TaggedMetricName(
                      "listeners", "http-requests.ignored", "port", String.valueOf(port)))
              .inc();
          writeHttpResponse(ctx, HttpResponseStatus.ACCEPTED, output, request);
          return;
        }
        try {
          reportChecks(jsonParser.readTree(requestBody), pointsPerRequest, output::append);
        } catch (Exception e) {
          status = HttpResponseStatus.BAD_REQUEST;
          output.append(errorMessageWithRootCause(e));
          logWarning("WF-300: Failed to handle /api/v1/check_run request", e, ctx);
        }
        writeHttpResponse(ctx, status, output, request);
        break;

      case "/api/v1/validate/":
        writeHttpResponse(ctx, HttpResponseStatus.OK, output, request);
        break;

      case "/intake/":
        try {
          status =
              processMetadataAndSystemMetrics(
                  jsonParser.readTree(requestBody),
                  processSystemMetrics,
                  pointsPerRequest,
                  output::append);
        } catch (Exception e) {
          status = HttpResponseStatus.BAD_REQUEST;
          output.append(errorMessageWithRootCause(e));
          logWarning("WF-300: Failed to handle /intake request", e, ctx);
        }
        httpRequestSize.update(pointsPerRequest.intValue());
        writeHttpResponse(ctx, status, output, request);
        break;

      default:
        writeHttpResponse(ctx, HttpResponseStatus.NO_CONTENT, output, request);
        logWarning(
            "WF-300: Unexpected path '" + request.uri() + "', returning HTTP 204", null, ctx);
        break;
    }
  }

  /**
   * Parse the metrics JSON and report the metrics found. There are 2 formats supported: array of
   * points and single point
   *
   * @param metrics a DataDog-format payload
   * @param pointCounter counter to track the number of points processed in one request
   * @return final HTTP status code to return to the client
   * @see #reportMetric(JsonNode, AtomicInteger, Consumer)
   */
  private HttpResponseStatus reportMetrics(
      final JsonNode metrics,
      @Nullable final AtomicInteger pointCounter,
      Consumer<String> outputConsumer) {
    if (metrics == null || !metrics.isObject()) {
      error("Empty or malformed /api/v1/series payload - ignoring", outputConsumer);
      return HttpResponseStatus.BAD_REQUEST;
    }
    if (!metrics.has("series")) {
      error("/api/v1/series payload missing 'series' field", outputConsumer);
      return HttpResponseStatus.BAD_REQUEST;
    }
    JsonNode series = metrics.get("series");
    if (!series.isArray()) {
      error("'series' field must be an array", outputConsumer);
      return HttpResponseStatus.BAD_REQUEST;
    }
    HttpResponseStatus worstStatus = HttpResponseStatus.ACCEPTED;
    for (final JsonNode metric : series) {
      HttpResponseStatus latestStatus = reportMetric(metric, pointCounter, outputConsumer);
      if (latestStatus.compareTo(worstStatus) > 0) {
        worstStatus = latestStatus;
      }
    }
    return worstStatus;
  }

  /**
   * Parse the individual timeseries object and send the metric to on to the point handler.
   *
   * @param metric the JSON object representing a single metric
   * @param pointCounter counter to track the number of points processed in one request
   * @return True if the metric was reported successfully; False o/w
   */
  private HttpResponseStatus reportMetric(
      final JsonNode metric,
      @Nullable final AtomicInteger pointCounter,
      Consumer<String> outputConsumer) {
    if (metric == null) {
      error("Skipping - series object null.", outputConsumer);
      return HttpResponseStatus.BAD_REQUEST;
    }
    try {
      if (metric.get("metric") == null) {
        error("Skipping - 'metric' field missing.", outputConsumer);
        return HttpResponseStatus.BAD_REQUEST;
      }
      String metricName =
          INVALID_METRIC_CHARACTERS.matcher(metric.get("metric").textValue()).replaceAll("_");
      String hostName =
          metric.get("host") == null ? "unknown" : metric.get("host").textValue().toLowerCase();
      JsonNode tagsNode = metric.get("tags");
      Map<String, String> systemTags;
      Map<String, String> tags = new HashMap<>();
      if ((systemTags = tagsCache.getIfPresent(hostName)) != null) {
        tags.putAll(systemTags);
      }
      extractTags(tagsNode, tags); // tags sent with the data override system host-level tags
      // Include a device= tag on the data if that property exists
      JsonNode deviceNode = metric.get("device");
      if (deviceNode != null) {
        tags.put("device", deviceNode.textValue());
      }
      // If the metric is of type rate its value needs to be multiplied by the specified interval
      int interval = 1;
      JsonNode type = metric.get("type");
      if (type != null) {
        if (type.textValue().equals("rate")) {
          JsonNode jsonInterval = metric.get("interval");
          if (jsonInterval != null && jsonInterval.isNumber()) {
            interval = jsonInterval.intValue();
          }
        }
      }
      JsonNode pointsNode = metric.get("points");
      if (pointsNode == null) {
        error("Skipping - 'points' field missing.", outputConsumer);
        return HttpResponseStatus.BAD_REQUEST;
      }
      for (JsonNode node : pointsNode) {
        if (node.size() == 2) {
          reportValue(
              metricName,
              hostName,
              tags,
              node.get(1),
              node.get(0).longValue() * 1000,
              pointCounter,
              interval);
        } else {
          error("Inconsistent point value size (expected: 2)", outputConsumer);
        }
      }
      return HttpResponseStatus.ACCEPTED;
    } catch (final Exception e) {
      logger.log(Level.WARNING, "Failed to add metric", e);
      outputConsumer.accept("Failed to add metric");
      return HttpResponseStatus.BAD_REQUEST;
    }
  }

  private void reportChecks(
      final JsonNode checkNode,
      @Nullable final AtomicInteger pointCounter,
      Consumer<String> outputConsumer) {
    if (checkNode == null) {
      error("Empty or malformed /api/v1/check_run payload - ignoring", outputConsumer);
      return;
    }
    if (checkNode.isArray()) {
      for (JsonNode check : checkNode) {
        reportCheck(check, pointCounter, outputConsumer);
      }
    } else {
      reportCheck(checkNode, pointCounter, outputConsumer);
    }
  }

  private void reportCheck(
      final JsonNode check,
      @Nullable final AtomicInteger pointCounter,
      Consumer<String> outputConsumer) {
    try {
      if (check.get("check") == null) {
        error("Skipping - 'check' field missing.", outputConsumer);
        return;
      }
      if (check.get("host_name") == null) {
        error("Skipping - 'host_name' field missing.", outputConsumer);
        return;
      }
      if (check.get("status") == null) {
        // ignore - there is no status to update
        return;
      }
      String metricName =
          INVALID_METRIC_CHARACTERS.matcher(check.get("check").textValue()).replaceAll("_");
      String hostName = check.get("host_name").textValue().toLowerCase();
      JsonNode tagsNode = check.get("tags");
      Map<String, String> systemTags;
      Map<String, String> tags = new HashMap<>();
      if ((systemTags = tagsCache.getIfPresent(hostName)) != null) {
        tags.putAll(systemTags);
      }
      extractTags(tagsNode, tags); // tags sent with the data override system host-level tags

      long timestamp =
          check.get("timestamp") == null ? Clock.now() : check.get("timestamp").asLong() * 1000;
      reportValue(metricName, hostName, tags, check.get("status"), timestamp, pointCounter);
    } catch (final Exception e) {
      logger.log(Level.WARNING, "WF-300: Failed to add metric", e);
    }
  }

  private HttpResponseStatus processMetadataAndSystemMetrics(
      final JsonNode metrics,
      boolean reportSystemMetrics,
      @Nullable final AtomicInteger pointCounter,
      Consumer<String> outputConsumer) {
    if (metrics == null || !metrics.isObject()) {
      error("Empty or malformed /intake payload", outputConsumer);
      return HttpResponseStatus.BAD_REQUEST;
    }
    if (!metrics.has("internalHostname")) {
      error("Payload missing 'internalHostname' field, ignoring", outputConsumer);
      return HttpResponseStatus.ACCEPTED;
    }

    // Some /api/v1/intake requests only contain host-tag metadata so process it first
    String hostName = metrics.get("internalHostname").textValue().toLowerCase();
    HashMap<String, String> systemTags = new HashMap<>();
    if (metrics.has("host-tags") && metrics.get("host-tags").get("system") != null) {
      extractTags(metrics.get("host-tags").get("system"), systemTags);
      // cache even if map is empty so we know how many unique hosts report metrics.
      tagsCache.put(hostName, systemTags);
      if (logger.isLoggable(Level.FINE)) {
        logger.fine("Cached system tags for " + hostName + ": " + systemTags.toString());
      }
    } else {
      Map<String, String> cachedTags = tagsCache.getIfPresent(hostName);
      if (cachedTags != null) {
        systemTags.clear();
        systemTags.putAll(cachedTags);
      }
    }

    if (!reportSystemMetrics) {
      Metrics.newCounter(
              new TaggedMetricName(
                  "listeners", "http-requests.ignored", "port", String.valueOf(port)))
          .inc();
      return HttpResponseStatus.ACCEPTED;
    }

    if (metrics.has("collection_timestamp")) {
      long timestamp = metrics.get("collection_timestamp").asLong() * 1000;

      // Report "system.io." metrics
      JsonNode ioStats = metrics.get("ioStats");
      if (ioStats != null && ioStats.isObject()) {
        ioStats
            .fields()
            .forEachRemaining(
                entry -> {
                  Map<String, String> deviceTags =
                      ImmutableMap.<String, String>builder()
                          .putAll(systemTags)
                          .put("device", entry.getKey())
                          .build();
                  if (entry.getValue() != null && entry.getValue().isObject()) {
                    entry
                        .getValue()
                        .fields()
                        .forEachRemaining(
                            metricEntry -> {
                              String metric =
                                  "system.io."
                                      + metricEntry
                                          .getKey()
                                          .replace('%', ' ')
                                          .replace('/', '_')
                                          .trim();
                              reportValue(
                                  metric,
                                  hostName,
                                  deviceTags,
                                  metricEntry.getValue(),
                                  timestamp,
                                  pointCounter);
                            });
                  }
                });
      }

      // Report all metrics that already start with "system."
      metrics
          .fields()
          .forEachRemaining(
              entry -> {
                if (entry.getKey().startsWith("system.")) {
                  reportValue(
                      entry.getKey(),
                      hostName,
                      systemTags,
                      entry.getValue(),
                      timestamp,
                      pointCounter);
                }
              });

      // Report CPU and memory metrics
      SYSTEM_METRICS.forEach(
          (key, value) ->
              reportValue(key, hostName, systemTags, metrics.get(value), timestamp, pointCounter));
    }
    return HttpResponseStatus.ACCEPTED;
  }

  private void reportValue(
      String metricName,
      String hostName,
      Map<String, String> tags,
      JsonNode valueNode,
      long timestamp,
      AtomicInteger pointCounter) {
    reportValue(metricName, hostName, tags, valueNode, timestamp, pointCounter, 1);
  }

  private void reportValue(
      String metricName,
      String hostName,
      Map<String, String> tags,
      JsonNode valueNode,
      long timestamp,
      AtomicInteger pointCounter,
      int interval) {
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

    // interval will normally be 1 unless the metric was a rate type with a specified interval
    value = value * interval;

    ReportPoint point =
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric(metricName)
            .setHost(hostName)
            .setTimestamp(timestamp)
            .setAnnotations(tags)
            .setValue(value)
            .build();
    if (pointCounter != null) {
      pointCounter.incrementAndGet();
    }
    if (preprocessorSupplier != null) {
      ReportableEntityPreprocessor preprocessor = preprocessorSupplier.get();
      String[] messageHolder = new String[1];
      preprocessor.forReportPoint().transform(point);
      if (!preprocessor.forReportPoint().filter(point, messageHolder)) {
        if (messageHolder[0] != null) {
          blockedPointsLogger.warning(ReportPointSerializer.pointToString(point));
          pointHandler.reject(point, messageHolder[0]);
        } else {
          blockedPointsLogger.info(ReportPointSerializer.pointToString(point));
          pointHandler.block(point);
        }
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
        tags.put("_source", input.substring(tagKvIndex + 1));
      } else {
        tags.put(
            INVALID_TAG_CHARACTERS.matcher(tagK).replaceAll("_"), input.substring(tagKvIndex + 1));
      }
    }
  }

  private void error(String msg, Consumer<String> outputConsumer) {
    pointHandler.reject((ReportPoint) null, msg);
    outputConsumer.accept(msg);
    outputConsumer.accept("\n");
  }
}
