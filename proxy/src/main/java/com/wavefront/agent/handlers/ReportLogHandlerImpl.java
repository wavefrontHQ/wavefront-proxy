package com.wavefront.agent.handlers;

import static com.wavefront.agent.LogsUtil.getOrCreateLogsCounterFromRegistry;
import static com.wavefront.agent.LogsUtil.getOrCreateLogsHistogramFromRegistry;
import static com.wavefront.data.Validation.validateLog;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.formatter.DataFormat;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.dto.Log;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.BurstRateTrackingCounter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

/**
 * This class will validate parsed logs and distribute them among SenderTask threads.
 *
 * @author amitw@vmware.com
 */
public class ReportLogHandlerImpl extends AbstractReportableEntityHandler<ReportLog, Log> {
  private static final Function<ReportLog, String> LOG_SERIALIZER =
      value -> new Log(value).toString();

  private final Logger validItemsLogger;
  final ValidationConfiguration validationConfig;
  private final MetricsRegistry registry;
  private DataFormat format;
  /**
   * @param senderTaskMap sender tasks.
   * @param handlerKey pipeline key.
   * @param blockedItemsPerBatch number of blocked items that are allowed to be written into the
   *     main log.
   * @param validationConfig validation configuration.
   * @param setupMetrics Whether we should report counter metrics.
   * @param receivedRateSink where to report received rate.
   * @param blockedLogsLogger logger for blocked logs.
   * @param validLogsLogger logger for valid logs.
   */
  public ReportLogHandlerImpl(
      final HandlerKey handlerKey,
      final int blockedItemsPerBatch,
      @Nullable final Map<String, Collection<SenderTask<Log>>> senderTaskMap,
      @Nonnull final ValidationConfiguration validationConfig,
      final boolean setupMetrics,
      @Nullable final BiConsumer<String, Long> receivedRateSink,
      @Nullable final Logger blockedLogsLogger,
      @Nullable final Logger validLogsLogger) {
    super(
        handlerKey,
        blockedItemsPerBatch,
        LOG_SERIALIZER,
        senderTaskMap,
        true,
        receivedRateSink,
        blockedLogsLogger);
    this.validItemsLogger = validLogsLogger;
    this.validationConfig = validationConfig;
    registry = setupMetrics ? Metrics.defaultRegistry() : LOCAL_REGISTRY;
  }

  @Override
  protected void initializeCounters() {
    this.blockedCounter =
        getOrCreateLogsCounterFromRegistry(registry, format, metricPrefix, "blocked");
    this.rejectedCounter =
        getOrCreateLogsCounterFromRegistry(registry, format, metricPrefix, "rejected");
    if (format == DataFormat.LOGS_JSON_CLOUDWATCH) {
      MetricName receivedMetricName =
          new MetricName(metricPrefix + "." + format.name().toLowerCase(), "", "received");
      registry.newCounter(receivedMetricName).inc();
      BurstRateTrackingCounter receivedStats =
          new BurstRateTrackingCounter(receivedMetricName, registry, 1000);
      registry.newGauge(
          new MetricName(
              metricPrefix + "." + format.name().toLowerCase(), "", "received.max-burst-rate"),
          new Gauge<Double>() {
            @Override
            public Double value() {
              return receivedStats.getMaxBurstRateAndClear();
            }
          });
    }
  }

  @Override
  protected void reportInternal(ReportLog log) {
    initializeCounters();
    getOrCreateLogsHistogramFromRegistry(registry, format, metricPrefix + ".received", "tagCount")
        .update(log.getAnnotations().size());

    getOrCreateLogsHistogramFromRegistry(
            registry, format, metricPrefix + ".received", "messageLength")
        .update(log.getMessage().length());

    Histogram receivedTagLength =
        getOrCreateLogsHistogramFromRegistry(
            registry, format, metricPrefix + ".received", "tagLength");
    for (Annotation a : log.getAnnotations()) {
      receivedTagLength.update(a.getValue().length());
    }

    validateLog(log, validationConfig);
    getOrCreateLogsHistogramFromRegistry(registry, format, metricPrefix + ".received", "lag")
        .update(Clock.now() - log.getTimestamp());

    Log logObj = new Log(log);
    getOrCreateLogsCounterFromRegistry(registry, format, metricPrefix + ".received", "bytes")
        .inc(logObj.getDataSize());
    getTask(APIContainer.CENTRAL_TENANT_NAME).add(logObj);
    getReceivedCounter().inc();
    attemptedCounter.inc();
    if (validItemsLogger != null && validItemsLogger.isLoggable(Level.FINEST)) {
      validItemsLogger.info(LOG_SERIALIZER.apply(log));
    }
    System.out.println(
        "We do not see a problem here: reportInternal(this is where we count "
            + "number of logs processed)");
  }

  @Override
  public void setLogFormat(DataFormat format) {
    this.format = format;
  }
}
