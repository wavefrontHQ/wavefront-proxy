package com.wavefront.agent.logsharvesting;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.agent.PointHandler;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.config.LogsIngestionConfig;
import com.wavefront.agent.config.MetricMatcher;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import org.logstash.beats.IMessageListener;
import org.logstash.beats.Message;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Level;
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
  // A map from "true" to the currently loaded logs ingestion config.
  @VisibleForTesting
  final LogsIngestionConfigManager logsIngestionConfigManager;
  private final Counter received, unparsed, parsed, sent, malformed;
  private final Histogram drift;
  private final Supplier<Long> currentMillis;
  private final MetricsReporter metricsReporter;
  private EvictingMetricsRegistry evictingMetricsRegistry;

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
    logsIngestionConfigManager = new LogsIngestionConfigManager(
        logsIngestionConfigSupplier,
        removedMetricMatcher -> evictingMetricsRegistry.evict(removedMetricMatcher));
    LogsIngestionConfig logsIngestionConfig = logsIngestionConfigManager.getConfig();

    this.evictingMetricsRegistry = new EvictingMetricsRegistry(
        logsIngestionConfig.expiryMillis, logsIngestionConfig.useWavefrontHistograms, currentMillis);

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

    // Continually flush user metrics to Wavefront.
    this.metricsReporter = new MetricsReporter(
        evictingMetricsRegistry.metricsRegistry(), flushProcessor, "FilebeatMetricsReporter", pointHandler, prefix);
    this.metricsReporter.start(config.aggregationIntervalSeconds, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  MetricsReporter getMetricsReporter() {
    return metricsReporter;
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
      success |= maybeIngestLog(evictingMetricsRegistry::getCounter, metricMatcher, filebeatMessage);
    }

    for (MetricMatcher metricMatcher : logsIngestionConfig.gauges) {
      success |= maybeIngestLog(evictingMetricsRegistry::getGauge, metricMatcher, filebeatMessage);
    }

    for (MetricMatcher metricMatcher : logsIngestionConfig.histograms) {
      success |= maybeIngestLog(evictingMetricsRegistry::getHistogram, metricMatcher, filebeatMessage);
    }

    if (success) {
      parsed.inc();
    } else {
      unparsed.inc();
    }
  }

  private boolean maybeIngestLog(
      BiFunction<MetricName, MetricMatcher, Metric> metricLoader, MetricMatcher metricMatcher,
      FilebeatMessage filebeatMessage) {
    Double[] output = {null};
    TimeSeries timeSeries = metricMatcher.timeSeries(filebeatMessage, output);
    if (timeSeries == null) return false;
    MetricName metricName = TimeSeriesUtils.toMetricName(timeSeries);
    Metric metric = metricLoader.apply(metricName, metricMatcher);
    try {
      metric.processWith(readProcessor, metricName, new ReadProcessorContext(output[0]));
    } catch (Exception e) {
      logger.severe("Could not process metric " + metricName.toString());
      e.printStackTrace();
    }
    return true;
  }

  @Override
  public void onNewConnection(ChannelHandlerContext ctx) {
    logger.info("New filebeat connection.");
  }

  @Override
  public void onConnectionClose(ChannelHandlerContext ctx) {
    logger.info("Filebeat connection closed.");
  }

  @Override
  public void onException(ChannelHandlerContext ctx, Throwable cause) {
    logger.log(Level.SEVERE, "Caught error processing beats data.", cause);
  }

  @Override
  public void onChannelInitializeException(ChannelHandlerContext ctx, Throwable cause) {
    logger.log(Level.SEVERE, "Caught initializing beats data processor.", cause);
  }
}
