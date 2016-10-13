package com.wavefront.agent.logsharvesting;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.wavefront.agent.PointHandler;
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

import java.util.Timer;
import java.util.TimerTask;
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
  private final FlushProcessor flushProcessor;
  private static final ReadProcessor readProcessor = new ReadProcessor();
  private final PointHandler pointHandler;
  private final MetricsRegistry metricsRegistry;
  private final LogsIngestionConfig logsIngestionConfig;
  private final LoadingCache<MetricName, Metric> metricCache;
  private final Counter received, unparsed, parsed, sent, malformed;
  private final Histogram drift;
  private final String prefix;
  private final Timer flushTimer;
  private final Supplier<Long> currentMillis;

  /**
   * @param pointHandler        Play parsed metrics and meta-metrics to this
   * @param logsIngestionConfig configuration object for logs harvesting
   * @param prefix              all harvested metrics start with this prefix
   * @param currentMillis       supplier of the current time in millis
   */
  public FilebeatListener(PointHandler pointHandler, LogsIngestionConfig logsIngestionConfig,
                          String prefix, Supplier<Long> currentMillis) {
    this.pointHandler = pointHandler;
    this.prefix = prefix;
    this.logsIngestionConfig = logsIngestionConfig;
    this.metricsRegistry = new MetricsRegistry();
    // Meta metrics.
    this.received = Metrics.newCounter(new MetricName("logsharvesting", "", "received"));
    this.unparsed = Metrics.newCounter(new MetricName("logsharvesting", "", "unparsed"));
    this.parsed = Metrics.newCounter(new MetricName("logsharvesting", "", "parsed"));
    this.malformed = Metrics.newCounter(new MetricName("logsharvesting", "", "malformed"));
    this.sent = Metrics.newCounter(new MetricName("logsharvesting", "", "sent"));
    this.drift = Metrics.newHistogram(new MetricName("logsharvesting", "", "drift"));
    this.currentMillis = currentMillis;
    this.flushProcessor = new FlushProcessor(sent, this.currentMillis);

    // Set up user specified metric harvesting.
    this.metricCache = Caffeine.<MetricName, Metric>newBuilder()
        .expireAfterAccess(logsIngestionConfig.expiryMillis, TimeUnit.MILLISECONDS)
        .removalListener((key, value, cause) -> {
          if (key instanceof MetricName) {
            MetricName metricName = (MetricName) key;
            metricsRegistry.removeMetric(metricName);
          } else {
            logger.severe("Unknown entry removed from metricsCache: " +
                (key == null ? "null" : key.getClass().getName()));
          }
        })
        .build(new MetricCacheLoader(this.metricsRegistry, this.currentMillis));

    flushTimer = new Timer();
    long flushMillis = TimeUnit.SECONDS.toMillis(logsIngestionConfig.aggregationIntervalSeconds);
    flushTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          flush();
        } catch (Exception e) {
          logger.severe("Error flushing: " + e.getMessage());
          e.printStackTrace();
        }
      }
    }, flushMillis, flushMillis);
  }

  /**
   * Send currently aggregated points to the sink.
   */
  @VisibleForTesting
  synchronized void flush() throws Exception {
    for (MetricName metricName : metricCache.asMap().keySet()) {
      Metric metric = metricCache.get(metricName);
      metric.processWith(flushProcessor, metricName,
          new FlushProcessorContext(TimeSeriesUtils.fromMetricName(metricName), prefix, pointHandler));
    }
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
      drift.update(System.currentTimeMillis() - filebeatMessage.getTimestampMillis());
    }

    Double[] output = {null};
    for (MetricMatcher metricMatcher : logsIngestionConfig.counters) {
      TimeSeries timeSeries = metricMatcher.timeSeries(filebeatMessage, output);
      if (timeSeries == null) continue;
      readMetric(Counter.class, timeSeries, output[0]);
      success = true;
    }

    for (MetricMatcher metricMatcher : logsIngestionConfig.gauges) {
      TimeSeries timeSeries = metricMatcher.timeSeries(filebeatMessage, output);
      if (timeSeries == null) continue;
      readMetric(Gauge.class, timeSeries, output[0]);
      success = true;
    }

    for (MetricMatcher metricMatcher : logsIngestionConfig.histograms) {
      TimeSeries timeSeries = metricMatcher.timeSeries(filebeatMessage, output);
      if (timeSeries == null) continue;
      readMetric(logsIngestionConfig.useWavefrontHistograms ? WavefrontHistogram.class : Histogram.class,
          timeSeries, output[0]);
      success = true;
    }
    if (!success) unparsed.inc();
  }

  private void readMetric(Class clazz, TimeSeries timeSeries, Double value) {
    MetricName metricName = TimeSeriesUtils.toMetricName(clazz, timeSeries);
    Metric metric = metricCache.get(metricName);
    try {
      metric.processWith(readProcessor, metricName, new ReadProcessorContext(value));
    } catch (Exception e) {
      logger.severe("Could not process metric " + metricName.toString());
      e.printStackTrace();
    }
    parsed.inc();
  }

  @Override
  public void onNewConnection(ChannelHandlerContext ctx) {}

  @Override
  public void onConnectionClose(ChannelHandlerContext ctx) {}

  @Override
  public void onException(ChannelHandlerContext ctx, Throwable cause) {}

  @Override
  public void onChannelInitializeException(ChannelHandlerContext ctx, Throwable cause) {}
}
