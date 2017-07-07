package com.wavefront.metrics;


import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.reporting.AbstractPollingReporter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Adapted from MetricsServlet.
 *
 * @author Sam Pullara (sam@wavefront.com)
 * @author Clement Pang (clement@wavefront.com)
 * @author Andrew Kao (andrew@wavefront.com)
 */
public class JsonMetricsReporter extends AbstractPollingReporter {

  private static final Logger logger = Logger.getLogger(JsonMetricsReporter.class.getCanonicalName());
  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

  private final boolean includeVMMetrics;
  private final String table;
  private final String sunnylabsHost;
  private final Integer sunnylabsPort;  // Null means use default URI port, probably 80 or 443.
  private final String host;
  private final Map<String, String> tags;
  private final Counter errors;
  private final boolean clearMetrics, https;
  private final MetricTranslator metricTranslator;

  private final OkHttpClient client = new OkHttpClient.Builder()
      .connectTimeout(10, TimeUnit.SECONDS)
      .writeTimeout(10, TimeUnit.SECONDS)
      .readTimeout(30, TimeUnit.SECONDS)
      .build();

  private Timer latency;
  private Counter reports;

  /**
   * Track and report uptime of services.
   */
  private final long START_TIME = System.currentTimeMillis();
  private final Gauge<Long> serverUptime = Metrics.newGauge(new TaggedMetricName("service", "uptime"),
      new Gauge<Long>() {
        @Override
        public Long value() {
          return System.currentTimeMillis() - START_TIME;
        }
      });

  public JsonMetricsReporter(MetricsRegistry registry, String table,
                             String sunnylabsHost, Map<String, String> tags, boolean clearMetrics)
      throws UnknownHostException {
    this(registry, true, table, sunnylabsHost, tags, clearMetrics);
  }

  public JsonMetricsReporter(MetricsRegistry registry, boolean includeVMMetrics,
                             String table, String sunnylabsHost, Map<String, String> tags, boolean clearMetrics)
      throws UnknownHostException {
    this(registry, includeVMMetrics, table, sunnylabsHost, tags, clearMetrics, true, null);
  }

  public JsonMetricsReporter(MetricsRegistry registry, boolean includeVMMetrics,
                             String table, String sunnylabsHost, Map<String, String> tags, boolean clearMetrics,
                             boolean https, MetricTranslator metricTranslator)
      throws UnknownHostException {
    super(registry, "json-metrics-reporter");
    this.metricTranslator = metricTranslator;
    this.includeVMMetrics = includeVMMetrics;
    this.tags = tags;
    this.table = table;

    if (sunnylabsHost.contains(":")) {
      int idx = sunnylabsHost.indexOf(":");
      String host = sunnylabsHost.substring(0, idx);
      String strPort = sunnylabsHost.substring(idx + 1);
      Integer port = null;
      this.sunnylabsHost = host;
      try {
        port = Integer.parseInt(strPort);
      } catch (NumberFormatException e) {
        logger.log(Level.SEVERE, "Cannot infer port for JSON reporting", e);
      }
      this.sunnylabsPort = port;
    } else {
      this.sunnylabsHost = sunnylabsHost;
      this.sunnylabsPort = null;
    }

    this.clearMetrics = clearMetrics;
    this.host = InetAddress.getLocalHost().getHostName();
    this.https = https;
    if (!this.https) {
      logger.severe("===================================================================");
      logger.severe("HTTPS is off for reporting! This should never be set in production!");
      logger.severe("===================================================================");
    }

    latency = Metrics.newTimer(new MetricName("jsonreporter", "jsonreporter", "latency"), MILLISECONDS, SECONDS);
    reports = Metrics.newCounter(new MetricName("jsonreporter", "jsonreporter", "reports"));
    errors = Metrics.newCounter(new MetricName("jsonreporter", "jsonreporter", "errors"));
  }

  @Override
  public void run() {
    try {
      reportMetrics();
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Uncaught exception in reportMetrics loop", t);
    }
  }

  public void reportMetrics() {
    TimerContext time = latency.time();
    try {
      UriBuilder builder = UriBuilder.fromUri(new URI(
          https ? "https" : "http", sunnylabsHost, "/report/metrics", null));
      if (sunnylabsPort != null) {
        builder.port(sunnylabsPort);
      }
      builder.queryParam("h", host);
      builder.queryParam("t", table);
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        builder.queryParam(tag.getKey(), tag.getValue());
      }
      URL http = builder.build().toURL();
      logger.info("Reporting metrics (JSON) to: " + http);
      Request request = new Request.Builder().
          url(http).
          post(new RequestBody() {
            @Nullable
            @Override
            public okhttp3.MediaType contentType() {
              return JSON;
            }

            @Override
            public void writeTo(BufferedSink bufferedSink) throws IOException {
              JsonMetricsGenerator.generateJsonMetrics(bufferedSink.outputStream(),
                  getMetricsRegistry(), includeVMMetrics, true,
                  clearMetrics, metricTranslator);
            }
          }).
          build();
      try (final Response response = client.newCall(request).execute()) {
        logger.info("Metrics (JSON) reported: " + response.code());
      }
      reports.inc();
    } catch (Throwable e) {
      logger.log(Level.WARNING, "Failed to report metrics (JSON)", e);
      errors.inc();
    } finally {
      time.stop();
    }
  }
}
