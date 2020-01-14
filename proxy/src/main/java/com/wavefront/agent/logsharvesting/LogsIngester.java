package com.wavefront.agent.logsharvesting;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Ticker;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.config.LogsIngestionConfig;
import com.wavefront.agent.config.MetricMatcher;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import wavefront.report.TimeSeries;

/**
 * Consumes log messages sent to {@link #ingestLog(LogsMessage)}. Configures and starts the periodic flush of
 * consumed metric data to Wavefront.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class LogsIngester {
  protected static final Logger logger = Logger.getLogger(LogsIngester.class.getCanonicalName());
  private static final ReadProcessor readProcessor = new ReadProcessor();
  private final FlushProcessor flushProcessor;
  // A map from "true" to the currently loaded logs ingestion config.
  @VisibleForTesting
  final LogsIngestionConfigManager logsIngestionConfigManager;
  private final Counter unparsed, parsed;
  private final Supplier<Long> currentMillis;
  private final MetricsReporter metricsReporter;
  private EvictingMetricsRegistry evictingMetricsRegistry;

  /**
   * Create an instance using system clock.
   *
   * @param handlerFactory              factory for point handlers and histogram handlers
   * @param logsIngestionConfigSupplier supplied configuration object for logs harvesting.
   *                                    May be reloaded. Must return "null" on any problems,
   *                                    as opposed to throwing.
   * @param prefix                      all harvested metrics start with this prefix
  */
  public LogsIngester(ReportableEntityHandlerFactory handlerFactory,
                      Supplier<LogsIngestionConfig> logsIngestionConfigSupplier,
                      String prefix) throws ConfigurationException {
    this(handlerFactory, logsIngestionConfigSupplier, prefix, System::currentTimeMillis,
        Ticker.systemTicker());
  }

  /**
   * Create an instance using provided clock and nano.
   *
   * @param handlerFactory              factory for point handlers and histogram handlers
   * @param logsIngestionConfigSupplier supplied configuration object for logs harvesting.
   *                                    May be reloaded. Must return "null" on any problems,
   *                                    as opposed to throwing.
   * @param prefix                      all harvested metrics start with this prefix
   * @param currentMillis               supplier of the current time in millis
   * @param ticker                      nanosecond-precision clock for Caffeine cache.
   * @throws ConfigurationException if the first config from logsIngestionConfigSupplier is null
   */
  @VisibleForTesting
  LogsIngester(ReportableEntityHandlerFactory handlerFactory,
               Supplier<LogsIngestionConfig> logsIngestionConfigSupplier, String prefix,
               Supplier<Long> currentMillis, Ticker ticker) throws ConfigurationException {
    logsIngestionConfigManager = new LogsIngestionConfigManager(
        logsIngestionConfigSupplier,
        removedMetricMatcher -> evictingMetricsRegistry.evict(removedMetricMatcher));
    LogsIngestionConfig logsIngestionConfig = logsIngestionConfigManager.getConfig();

    MetricsRegistry metricsRegistry = new MetricsRegistry();
    this.evictingMetricsRegistry = new EvictingMetricsRegistry(metricsRegistry,
        logsIngestionConfig.expiryMillis, true, logsIngestionConfig.useDeltaCounters,
        currentMillis, ticker);

    // Logs harvesting metrics.
    this.unparsed = Metrics.newCounter(new MetricName("logsharvesting", "", "unparsed"));
    this.parsed = Metrics.newCounter(new MetricName("logsharvesting", "", "parsed"));
    this.currentMillis = currentMillis;
    this.flushProcessor = new FlushProcessor(currentMillis,
        logsIngestionConfig.useWavefrontHistograms, logsIngestionConfig.reportEmptyHistogramStats);

    // Continually flush user metrics to Wavefront.
    this.metricsReporter = new MetricsReporter(metricsRegistry, flushProcessor,
        "FilebeatMetricsReporter", handlerFactory, prefix);
  }

  public void start() {
    long interval = this.logsIngestionConfigManager.getConfig().aggregationIntervalSeconds;
    this.metricsReporter.start(interval, TimeUnit.SECONDS);
    // check for expired cached items and trigger evictions every 2x aggregationIntervalSeconds
    // but no more than once a minute. This is a workaround for the issue that surfaces mostly
    // during testing, when there are no matching log messages at all for more than expiryMillis,
    // which means there is no cache access and no time-based evictions are performed.
    Executors.newSingleThreadScheduledExecutor().
        scheduleWithFixedDelay(evictingMetricsRegistry::cleanUp, interval * 3 / 2,
            Math.max(60, interval * 2), TimeUnit.SECONDS);
  }

  public void flush() {
    this.metricsReporter.run();
  }

  @VisibleForTesting
  MetricsReporter getMetricsReporter() {
    return metricsReporter;
  }

  public void ingestLog(LogsMessage logsMessage) {
    LogsIngestionConfig logsIngestionConfig = logsIngestionConfigManager.getConfig();

    boolean success = false;
    for (MetricMatcher metricMatcher : logsIngestionConfig.counters) {
      success |= maybeIngestLog(evictingMetricsRegistry::getCounter, metricMatcher, logsMessage);
    }

    for (MetricMatcher metricMatcher : logsIngestionConfig.gauges) {
      success |= maybeIngestLog(evictingMetricsRegistry::getGauge, metricMatcher, logsMessage);
    }

    for (MetricMatcher metricMatcher : logsIngestionConfig.histograms) {
      success |= maybeIngestLog(evictingMetricsRegistry::getHistogram, metricMatcher, logsMessage);
    }

    if (success) {
      parsed.inc();
    } else {
      unparsed.inc();
    }
  }

  private boolean maybeIngestLog(
      BiFunction<MetricName, MetricMatcher, Metric> metricLoader, MetricMatcher metricMatcher,
      LogsMessage logsMessage) {
    Double[] output = {null};
    TimeSeries timeSeries = metricMatcher.timeSeries(logsMessage, output);
    if (timeSeries == null) return false;
    MetricName metricName = TimeSeriesUtils.toMetricName(timeSeries);
    Metric metric = metricLoader.apply(metricName, metricMatcher);
    try {
      metric.processWith(readProcessor, metricName, new ReadProcessorContext(output[0]));
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Could not process metric " + metricName.toString(), e);
    }
    return true;
  }
}
