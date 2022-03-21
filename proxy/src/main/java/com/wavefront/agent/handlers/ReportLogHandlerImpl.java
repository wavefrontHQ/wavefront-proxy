package com.wavefront.agent.handlers;

import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.dto.Log;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.ReportLog;

import static com.wavefront.data.Validation.validateLog;

/**
 * This class will validate parsed logs and distribute them among SenderTask threads.
 *
 * @author amitw@vmware.com
 */
public class ReportLogHandlerImpl extends AbstractReportableEntityHandler<ReportLog, Log> {
  private static final Function<ReportLog, String> LOG_SERIALIZER = value -> new Log(value).toString();

  private final Logger validItemsLogger;
  final ValidationConfiguration validationConfig;
  final com.yammer.metrics.core.Histogram receivedLogLag;
  final com.yammer.metrics.core.Histogram receivedTagCount;
  final com.yammer.metrics.core.Histogram receivedByteCount;

  /**
   * @param handlerKey           pipeline key.
   * @param blockedItemsPerBatch number of blocked items that are allowed to be written into the
   *                             main log.
   * @param senderTasks          sender tasks.
   * @param validationConfig     validation configuration.
   * @param setupMetrics         Whether we should report counter metrics.
   * @param receivedRateSink     where to report received rate.
   * @param blockedLogsLogger    logger for blocked logs.
   * @param validLogsLogger      logger for valid logs.
   */
  public ReportLogHandlerImpl(final HandlerKey handlerKey, final int blockedItemsPerBatch,
                          @Nullable final Collection<SenderTask<Log>> senderTasks,
                          @Nonnull final ValidationConfiguration validationConfig,
                          final boolean setupMetrics,
                          @Nullable final Consumer<Long> receivedRateSink,
                          @Nullable final Logger blockedLogsLogger,
                          @Nullable final Logger validLogsLogger) {
    super(handlerKey, blockedItemsPerBatch, LOG_SERIALIZER, senderTasks, true, receivedRateSink,
        blockedLogsLogger);
    this.validItemsLogger = validLogsLogger;
    this.validationConfig = validationConfig;
    MetricsRegistry registry = setupMetrics ? Metrics.defaultRegistry() : LOCAL_REGISTRY;
    this.receivedLogLag = registry.newHistogram(new MetricName(handlerKey.toString() +
        ".received", "", "lag"), false);
    this.receivedTagCount = registry.newHistogram(new MetricName(handlerKey.toString() +
        ".received", "", "tagCount"), false);
    this.receivedByteCount = registry.newHistogram(new MetricName(handlerKey.toString() +
        ".received", "", "byteCount"), false);
  }

  @Override
  protected void reportInternal(ReportLog log) {
    receivedTagCount.update(log.getAnnotations().size());
    validateLog(log, validationConfig);
    receivedLogLag.update(Clock.now() - log.getTimestamp());
    Log logObj = new Log(log);
    receivedByteCount.update(logObj.toString().length());
    getTask().add(logObj);
    getReceivedCounter().inc();
    if (validItemsLogger != null && validItemsLogger.isLoggable(Level.FINEST)) {
      validItemsLogger.info(LOG_SERIALIZER.apply(log));
    }
  }
}
