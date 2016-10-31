package com.wavefront.agent.logsharvesting;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.config.LogsIngestionConfig;
import com.wavefront.agent.config.MetricMatcher;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.WavefrontHistogram;

import org.logstash.beats.IMessageListener;
import org.logstash.beats.Message;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;

import io.netty.channel.ChannelHandlerContext;
import sunnylabs.report.TimeSeries;

/**
 * Listens for messages from Filebeat and processes them, sending telemetry to a given sink.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FilebeatListener implements IMessageListener {
  protected static final Logger logger = Logger.getLogger(FilebeatListener.class.getCanonicalName());
  private static final ReadProcessor readProcessor = new ReadProcessor();
  private final FlushProcessor flushProcessor;
  private final PointHandler pointHandler;
  private final MetricsRegistry metricsRegistry;
  // A map from "true" to the currently loaded logs ingestion config.
  @VisibleForTesting
  final LogsIngestionConfigManager logsIngestionConfigManager;
  private LoadingCache<MetricName, Metric> metricCache;
  private final Counter received, unparsed, parsed, sent, malformed;
  private final Histogram drift;
  private final Supplier<Long> currentMillis;
  private final MetricsReporter metricsReporter;
  // Keys are MetricMatcher.toString(), values are TimeSeries that the given metric matcher has emitted.
  private final LoadingCache<MetricMatcher, List<MetricName>> timeSeriesForMetricMatchers;

  /**
   * @param pointHandler                play parsed metrics
   * @param logsIngestionConfigSupplier supplied configuration object for logs harvesting. May be reloaded. Must return
   *                                    "null" on any problems, as opposed to throwing
   * @param prefix                      all harvested metrics start with this prefix
   * @param currentMillis               supplier of the current time in millis
   * @throws ConfigurationException if the first config from logsIngestionConfigSupplier is null
   */
  public FilebeatListener(PointHandler pointHandler, Supplier<LogsIngestionConfig> logsIngestionConfigSupplier,
                          String prefix, Supplier<Long> currentMillis) throws ConfigurationException {
    this.timeSeriesForMetricMatchers = Caffeine.<MetricMatcher, List<TimeSeries>>newBuilder()
        .build((ignored) -> Lists.newLinkedList());
    this.metricsRegistry = new MetricsRegistry();
    logsIngestionConfigManager = new LogsIngestionConfigManager(
        logsIngestionConfigSupplier,
        (removedMetricMatcher -> {
          List<MetricName> removedMetricNames = timeSeriesForMetricMatchers.get(removedMetricMatcher);
          for (MetricName removed : removedMetricNames) {
            metricCache.invalidate(removed);
          }
          timeSeriesForMetricMatchers.invalidate(removedMetricMatcher);
        }));

    // Logs harvesting metrics.
    this.received = Metrics.newCounter(new MetricName("logsharvesting", "", "received"));
    this.unparsed = Metrics.newCounter(new MetricName("logsharvesting", "", "unparsed"));
    this.parsed = Metrics.newCounter(new MetricName("logsharvesting", "", "parsed"));
    this.malformed = Metrics.newCounter(new MetricName("logsharvesting", "", "malformed"));
    this.sent = Metrics.newCounter(new MetricName("logsharvesting", "", "sent"));
    this.drift = Metrics.newHistogram(new MetricName("logsharvesting", "", "drift"));
    this.currentMillis = currentMillis;
    this.flushProcessor = new FlushProcessor(sent, currentMillis);

    // Set up user specified metric harvesting.
    LogsIngestionConfig config = logsIngestionConfigManager.getConfig();
    this.pointHandler = pointHandler;
    this.metricCache = Caffeine.<MetricName, Metric>newBuilder()
        .expireAfterAccess(config.expiryMillis, TimeUnit.MILLISECONDS)
        .<MetricName, Metric>removalListener((metricName, metric, reason) -> {
          if (metricName == null || metric == null) {
            logger.severe("Application error, pulled null key or value from metricCache.");
            return;
          }
          metricsRegistry.removeMetric(metricName);
        })
        .build(new MetricCacheLoader(metricsRegistry, currentMillis));

    // Continually flush user metrics to Wavefront.
    this.metricsReporter = new MetricsReporter(
        metricsRegistry, flushProcessor, "FilebeatMetricsReporter", pointHandler, prefix);
    this.metricsReporter.start(config.aggregationIntervalSeconds, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  MetricsReporter getMetricsReporter() {
    return metricsReporter;
  }

  @VisibleForTesting
  LoadingCache<MetricName, Metric> getMetricCache() {
    return metricCache;
  }

  @Override
  public void onNewMessage(ChannelHandlerContext ctx, Message message) {
    received.inc();
    FilebeatMessage filebeatMessage;
    boolean success = false;
    try {
      filebeatMessage = new FilebeatMessage(message);
    } catch (MalformedMessageException exn) {
      logger.severe("Malformed message received from filebeat, dropping.");
      malformed.inc();
      return;
    }

    if (filebeatMessage.getTimestampMillis() != null) {
      drift.update(currentMillis.get() - filebeatMessage.getTimestampMillis());
    }

    LogsIngestionConfig logsIngestionConfig = logsIngestionConfigManager.getConfig();

    for (MetricMatcher metricMatcher : logsIngestionConfig.counters) {
      success |= maybeIngestLog(Counter.class, metricMatcher, filebeatMessage);
    }

    for (MetricMatcher metricMatcher : logsIngestionConfig.gauges) {
      success |= maybeIngestLog(Gauge.class, metricMatcher, filebeatMessage);
    }

    for (MetricMatcher metricMatcher : logsIngestionConfig.histograms) {
      success |= maybeIngestLog(
          logsIngestionConfig.useWavefrontHistograms ? WavefrontHistogram.class : Histogram.class,
          metricMatcher, filebeatMessage);
    }

    if (!success) unparsed.inc();
  }

  private boolean maybeIngestLog(Class<?> clazz, MetricMatcher metricMatcher, FilebeatMessage filebeatMessage) {
    Double[] output = {null};
    TimeSeries timeSeries = metricMatcher.timeSeries(filebeatMessage, output);
    if (timeSeries == null) return false;
    MetricName metricName = readMetric(clazz, timeSeries, output[0]);
    timeSeriesForMetricMatchers.get(metricMatcher).add(metricName);
    return true;
  }

  private MetricName readMetric(Class clazz, TimeSeries timeSeries, Double value) {
    MetricName metricName = TimeSeriesUtils.toMetricName(clazz, timeSeries);
    Metric metric = metricCache.get(metricName);
    try {
      metric.processWith(readProcessor, metricName, new ReadProcessorContext(value));
    } catch (Exception e) {
      logger.severe("Could not process metric " + metricName.toString());
      e.printStackTrace();
    }
    parsed.inc();
    return metricName;
  }

  @Override
  public void onNewConnection(ChannelHandlerContext ctx) {
  }

  @Override
  public void onConnectionClose(ChannelHandlerContext ctx) {
  }

  @Override
  public void onException(ChannelHandlerContext ctx, Throwable cause) {
  }

  @Override
  public void onChannelInitializeException(ChannelHandlerContext ctx, Throwable cause) {
  }
}
