package com.wavefront.agent.core.handlers;

import com.google.common.util.concurrent.RateLimiter;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.senders.SenderTask;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.jetbrains.annotations.NotNull;
import wavefront.report.ReportPoint;

/**
 * Base class for all {@link ReportableEntityHandler} implementations.
 *
 * @author vasily@wavefront.com
 * @param <T> the type of input objects handled
 * @param <U> the type of the output object as handled by {@link SenderTask}
 */
abstract class AbstractReportableEntityHandler<T, U> implements ReportableEntityHandler<T> {
  protected static final String MULTICASTING_TENANT_TAG_KEY = "multicastingTenantName";
  private static final Logger logger =
      Logger.getLogger(AbstractReportableEntityHandler.class.getCanonicalName());
  final QueueInfo queue;
  final String handler;

  @SuppressWarnings("UnstableApiUsage")
  final RateLimiter blockedItemsLimiter;

  final Function<T, String> serializer;
  final String rateUnit;
  final BurstRateTrackingCounter receivedStats;
  private final Logger blockedItemsLogger;
  private final Counter receivedCounter;
  private final Counter blockedCounter;
  private final Counter rejectedCounter;
  private final Timer timer;

  @SuppressWarnings("UnstableApiUsage")
  private final RateLimiter noDataStatsRateLimiter = RateLimiter.create(1.0d / 60);

  /**
   * @param queue metrics pipeline key (entity type + port number)
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into
   *     the main log file.
   * @param serializer helper function to convert objects to string. Used when writing blocked
   *     points to logs.
   * @param blockedItemsLogger a {@link Logger} instance for blocked items
   */
  AbstractReportableEntityHandler(
      String handler,
      @NotNull QueueInfo queue,
      final int blockedItemsPerBatch,
      final Function<T, String> serializer,
      @Nullable final Logger blockedItemsLogger) {
    this.handler = handler;
    this.queue = queue;
    //noinspection UnstableApiUsage
    this.blockedItemsLimiter =
        blockedItemsPerBatch == 0 ? null : RateLimiter.create(blockedItemsPerBatch / 10d);
    this.serializer = serializer;
    this.rateUnit = queue.getEntityType().getRateUnit();
    this.blockedItemsLogger = blockedItemsLogger;

    MetricsRegistry registry = Metrics.defaultRegistry();
    String metricPrefix = queue.getName() + "." + this.handler;
    MetricName receivedMetricName = new MetricName(metricPrefix, "", "received");
    this.receivedCounter = registry.newCounter(receivedMetricName);
    this.blockedCounter = registry.newCounter(new MetricName(metricPrefix, "", "blocked"));
    this.rejectedCounter = registry.newCounter(new MetricName(metricPrefix, "", "rejected"));
    this.receivedStats = new BurstRateTrackingCounter(receivedMetricName, registry, 1000);

    registry.newGauge(
        new MetricName(metricPrefix + ".received", "", "max-burst-rate"),
        new Gauge<Double>() {
          @Override
          public Double value() {
            return receivedStats.getMaxBurstRateAndClear();
          }
        });

    timer = new Timer("stats-output-" + queue.getName() + "." + this.handler);
    timer.scheduleAtFixedRate(
        new TimerTask() {
          @Override
          public void run() {
            printStats();
          }
        },
        10_000,
        10_000);
    timer.scheduleAtFixedRate(
        new TimerTask() {
          @Override
          public void run() {
            printTotal();
          }
        },
        60_000,
        60_000);
  }

  @Override
  public void reject(@Nullable T item, @Nullable String message) {
    rejectedCounter.inc();
    if (item != null && blockedItemsLogger != null) {
      blockedItemsLogger.warning(serializer.apply(item));
    }
    //noinspection UnstableApiUsage
    if (message != null && blockedItemsLimiter != null && blockedItemsLimiter.tryAcquire()) {
      logger.info("[" + this.handler + "] blocked input: [" + message + "]");
    }
  }

  @Override
  public void reject(@Nonnull String line, @Nullable String message) {
    rejectedCounter.inc();
    if (blockedItemsLogger != null) blockedItemsLogger.warning(line);
    //noinspection UnstableApiUsage
    if (message != null && blockedItemsLimiter != null && blockedItemsLimiter.tryAcquire()) {
      logger.info("[" + this.handler + "] blocked input: [" + message + "]");
    }
  }

  @Override
  public void block(T item) {
    blockedCounter.inc();
    if (blockedItemsLogger != null) {
      blockedItemsLogger.info(serializer.apply(item));
    }
  }

  @Override
  public void block(@Nullable T item, @Nullable String message) {
    blockedCounter.inc();
    if (item != null && blockedItemsLogger != null) {
      blockedItemsLogger.info(serializer.apply(item));
    }
    if (message != null && blockedItemsLogger != null) {
      blockedItemsLogger.info(message);
    }
  }

  @Override
  public void report(T item) {
    try {
      reportInternal(item);
    } catch (IllegalArgumentException e) {
      this.reject(item, e.getMessage() + " (" + serializer.apply(item) + ")");
    } catch (Exception ex) {
      logger.log(
          Level.SEVERE,
          "WF-500 Uncaught exception when handling input (" + serializer.apply(item) + ")",
          ex);
    }
  }

  @Override
  public void shutdown() {
    if (this.timer != null) timer.cancel();
  }

  abstract void reportInternal(T item);

  final void reportToTenants(ReportPoint point) {}

  protected Counter getReceivedCounter() {
    return receivedCounter;
  }

  protected void printStats() {
    logger.info(
        "["
            + this.handler
            + "] "
            + queue.getEntityType().toCapitalizedString()
            + " received rate: "
            + receivedStats.getOneMinutePrintableRate()
            + " "
            + rateUnit
            + " (1 min), "
            + receivedStats.getFiveMinutePrintableRate()
            + " "
            + rateUnit
            + " (5 min), "
            + receivedStats.getCurrentRate()
            + " "
            + rateUnit
            + " (current).");
  }

  protected void printTotal() {
    logger.info(
        "["
            + this.handler
            + "] "
            + queue.getEntityType().toCapitalizedString()
            + " received since start: "
            + this.receivedCounter.count()
            + "; rejected: "
            + this.rejectedCounter.count()
            + "; blocked: "
            + this.blockedCounter.count());
  }
}
