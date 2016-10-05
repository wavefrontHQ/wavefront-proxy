package com.wavefront.agent.logsharvesting;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.config.LogsIngestionConfig;
import com.wavefront.agent.config.MetricMatcher;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.logstash.beats.IMessageListener;
import org.logstash.beats.Message;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.netty.channel.ChannelHandlerContext;
import sunnylabs.report.ReportPoint;
import sunnylabs.report.TimeSeries;

/**
 * Listens for messages from Filebeat and processes them, sending telemetry to a given sink.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FilebeatListener implements IMessageListener {
  protected static final Logger logger = Logger.getLogger(FilebeatListener.class.getCanonicalName());
  private static final FlushProcessor flushProcessor = new FlushProcessor();
  private static ReadProcessor readProcessor = new ReadProcessor();
  private final PointHandler pointHandler;
  private final MetricsRegistry metricsRegistry;
  private final LogsIngestionConfig logsIngestionConfig;
  private final LoadingCache<MetricName, Metric> metricCache;
  private final Counter received, evicted, unparsed, batchesProcessed, parsed;
  private final String hostname, prefix;

  /**
   * @param pointHandler        Play parsed metrics and meta-metrics to this
   * @param logsIngestionConfig configuration object for logs harvesting
   * @param hostname            hostname for meta-metrics (so, for this proxy)
   * @param prefix              all harvested metrics start with this prefix
   */
  public FilebeatListener(PointHandler pointHandler, LogsIngestionConfig logsIngestionConfig, String hostname,
                          String prefix) {
    this.pointHandler = pointHandler;
    this.hostname = hostname;
    this.prefix = prefix;
    this.logsIngestionConfig = logsIngestionConfig;
    this.metricsRegistry = new MetricsRegistry();
    // Meta metrics.
    this.received = Metrics.newCounter(FilebeatListener.class, "logsharvesting.received");
    this.evicted = Metrics.newCounter(FilebeatListener.class, "logsharvesting.evicted");
    this.unparsed = Metrics.newCounter(FilebeatListener.class, "logsharvesting.unparsed");
    this.parsed = Metrics.newCounter(FilebeatListener.class, "logsharvesting.parsed");
    this.batchesProcessed = Metrics.newCounter(FilebeatListener.class, "logsharvesting.unparsed");

    // Set up user specified metric harvesting.
    this.metricCache = Caffeine.<MetricName, Metric>newBuilder()
        .softValues()
        .expireAfterWrite(logsIngestionConfig.aggregationIntervalSeconds, TimeUnit.SECONDS)
        .removalListener((key, value, cause) -> {
          MetricName metricName = (MetricName) key;
          Metric metric = (Metric) value;
          if (cause == RemovalCause.COLLECTED) {
            metricsRegistry.removeMetric(metricName);
            evicted.inc();
          } else if (metric != null) {
            try {
              metric.processWith(flushProcessor, metricName, new FlushProcessorContext(
                  TimeSeriesUtils.fromMetricName(metricName), prefix, this.pointHandler));
            } catch (Exception e) {
              e.printStackTrace();
            }
          } else {
            logger.severe("Null metric retrieved from cache: " + metricName.toString());
          }
        })
        .build(new MetricCacheLoader(this.metricsRegistry));
  }

  /**
   * Send currently aggregated points to the sink.
   */
  @VisibleForTesting
  synchronized void flush() throws Exception {
    for (MetricName metricName : metricCache.asMap().keySet()) {
      Metric metric = metricCache.get(metricName);
      metric.processWith(new FlushProcessor(), metricName,
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
      return;
    }

    Double[] output = {null};
    for (MetricMatcher metricMatcher : logsIngestionConfig.counters) {
      TimeSeries timeSeries = metricMatcher.timeSeries(filebeatMessage, output);
      if (timeSeries == null) continue;
      readMetric(Counter.class, timeSeries, output[0]);
      success = true;
    }
    if (success) return;

    for (MetricMatcher metricMatcher : logsIngestionConfig.gauges) {
      TimeSeries timeSeries = metricMatcher.timeSeries(filebeatMessage, output);
      if (timeSeries == null) continue;
      readMetric(Gauge.class, timeSeries, output[0]);
      success = true;
    }
    if (success) return;

    unparsed.inc();
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
