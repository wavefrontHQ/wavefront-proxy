package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.BurstRateTrackingCounter;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base class for all {@link ReportableEntityHandler} implementations.
 *
 * @author vasily@wavefront.com
 *
 * @param <T> the type of input objects handled
 * @param <U> the type of the output object as handled by {@link SenderTask<U>}
 *
 */
abstract class AbstractReportableEntityHandler<T, U> implements ReportableEntityHandler<T, U> {
  private static final Logger logger = Logger.getLogger(
      AbstractReportableEntityHandler.class.getCanonicalName());
  protected static final MetricsRegistry LOCAL_REGISTRY = new MetricsRegistry();

  private final Logger blockedItemsLogger;

  final HandlerKey handlerKey;
  private final Counter receivedCounter;
  private final Counter attemptedCounter;
  private final Counter blockedCounter;
  private final Counter rejectedCounter;

  @SuppressWarnings("UnstableApiUsage")
  final RateLimiter blockedItemsLimiter;
  final Function<T, String> serializer;
  final List<SenderTask<U>> senderTasks;
  final boolean reportReceivedStats;
  final String rateUnit;

  final BurstRateTrackingCounter receivedStats;
  final BurstRateTrackingCounter deliveredStats;

  private final Timer timer;
  private final AtomicLong roundRobinCounter = new AtomicLong();
  @SuppressWarnings("UnstableApiUsage")
  private final RateLimiter noDataStatsRateLimiter = RateLimiter.create(1.0d / 60);

  /**
   * @param handlerKey           metrics pipeline key (entity type + port number)
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written
   *                             into the main log file.
   * @param serializer           helper function to convert objects to string. Used when writing
   *                             blocked points to logs.
   * @param senderTasks          tasks actually handling data transfer to the Wavefront endpoint.
   * @param reportReceivedStats  Whether we should report a .received counter metric.
   * @param receivedRateSink     Where to report received rate.
   * @param blockedItemsLogger   a {@link Logger} instance for blocked items
   */
  AbstractReportableEntityHandler(HandlerKey handlerKey,
                                  final int blockedItemsPerBatch,
                                  final Function<T, String> serializer,
                                  @Nullable final Collection<SenderTask<U>> senderTasks,
                                  boolean reportReceivedStats,
                                  @Nullable final Consumer<Long> receivedRateSink,
                                  @Nullable final Logger blockedItemsLogger) {
    this.handlerKey = handlerKey;
    //noinspection UnstableApiUsage
    this.blockedItemsLimiter = blockedItemsPerBatch == 0 ? null :
        RateLimiter.create(blockedItemsPerBatch / 10d);
    this.serializer = serializer;
    this.senderTasks = senderTasks == null ? new ArrayList<>() : new ArrayList<>(senderTasks);
    this.reportReceivedStats = reportReceivedStats;
    this.rateUnit = handlerKey.getEntityType().getRateUnit();
    this.blockedItemsLogger = blockedItemsLogger;

    MetricsRegistry registry = reportReceivedStats ? Metrics.defaultRegistry() : LOCAL_REGISTRY;
    String metricPrefix = handlerKey.toString();
    MetricName receivedMetricName = new MetricName(metricPrefix, "", "received");
    MetricName deliveredMetricName = new MetricName(metricPrefix, "", "delivered");
    this.receivedCounter = registry.newCounter(receivedMetricName);
    this.attemptedCounter = Metrics.newCounter(new MetricName(metricPrefix, "", "sent"));
    this.blockedCounter = registry.newCounter(new MetricName(metricPrefix, "", "blocked"));
    this.rejectedCounter = registry.newCounter(new MetricName(metricPrefix, "", "rejected"));
    this.receivedStats = new BurstRateTrackingCounter(receivedMetricName, registry, 1000);
    this.deliveredStats = new BurstRateTrackingCounter(deliveredMetricName, registry, 1000);
    registry.newGauge(new MetricName(metricPrefix + ".received", "", "max-burst-rate"), new Gauge<Double>() {
      @Override
      public Double value() {
        return receivedStats.getMaxBurstRateAndClear();
      }
    });
    timer = new Timer("stats-output-" + handlerKey);
    if (receivedRateSink != null) {
      timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          receivedRateSink.accept(receivedStats.getCurrentRate());
        }
      }, 1000, 1000);
    }
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        printStats();
      }
    }, 10_000, 10_000);
    if (reportReceivedStats) {
      timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          printTotal();
        }
      }, 60_000, 60_000);
    }
  }

  @Override
  public void reject(@Nullable T item, @Nullable String message) {
    blockedCounter.inc();
    rejectedCounter.inc();
    if (item != null && blockedItemsLogger != null) {
      blockedItemsLogger.warning(serializer.apply(item));
    }
    //noinspection UnstableApiUsage
    if (message != null && blockedItemsLimiter != null && blockedItemsLimiter.tryAcquire()) {
      logger.info("[" + handlerKey.getHandle() + "] blocked input: [" + message + "]");
    }
  }

  @Override
  public void reject(@Nonnull String line, @Nullable String message) {
    blockedCounter.inc();
    rejectedCounter.inc();
    if (blockedItemsLogger != null) blockedItemsLogger.warning(line);
    //noinspection UnstableApiUsage
    if (message != null && blockedItemsLimiter != null && blockedItemsLimiter.tryAcquire()) {
      logger.info("[" + handlerKey.getHandle() + "] blocked input: [" + message + "]");
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
      logger.log(Level.SEVERE, "WF-500 Uncaught exception when handling input (" +
          serializer.apply(item) + ")", ex);
    }
  }

  @Override
  public void shutdown() {
    if (this.timer != null) timer.cancel();
  }

  abstract void reportInternal(T item);

  protected Counter getReceivedCounter() {
    return receivedCounter;
  }

  protected SenderTask<U> getTask() {
    if (senderTasks == null) {
      throw new IllegalStateException("getTask() cannot be called on null senderTasks");
    }
    // roundrobin all tasks, skipping the worst one (usually with the highest number of points)
    int nextTaskId = (int)(roundRobinCounter.getAndIncrement() % senderTasks.size());
    long worstScore = 0L;
    int worstTaskId = 0;
    for (int i = 0; i < senderTasks.size(); i++) {
      long score = senderTasks.get(i).getTaskRelativeScore();
      if (score > worstScore) {
        worstScore = score;
        worstTaskId = i;
      }
    }
    if (nextTaskId == worstTaskId) {
      nextTaskId = (int)(roundRobinCounter.getAndIncrement() % senderTasks.size());
    }
    return senderTasks.get(nextTaskId);
  }

  protected void printStats() {
    // if we received no data over the last 5 minutes, only print stats once a minute
    //noinspection UnstableApiUsage
    if (receivedStats.getFiveMinuteCount() == 0 && !noDataStatsRateLimiter.tryAcquire()) return;
    if (reportReceivedStats) {
      logger.info("[" + handlerKey.getHandle() + "] " +
          handlerKey.getEntityType().toCapitalizedString() + " received rate: " +
          receivedStats.getOneMinutePrintableRate() + " " + rateUnit + " (1 min), " +
          receivedStats.getFiveMinutePrintableRate() + " " + rateUnit + " (5 min), " +
          receivedStats.getCurrentRate() + " " + rateUnit + " (current).");
    }
    if (deliveredStats.getFiveMinuteCount() == 0) return;
    logger.info("[" + handlerKey.getHandle() + "] " +
        handlerKey.getEntityType().toCapitalizedString() + " delivered rate: " +
        deliveredStats.getOneMinutePrintableRate() + " " + rateUnit + " (1 min), " +
        deliveredStats.getFiveMinutePrintableRate() + " " + rateUnit + " (5 min)");
    // we are not going to display current delivered rate because it _will_ be misinterpreted.
  }

  protected void printTotal() {
    logger.info("[" + handlerKey.getHandle() + "] " +
        handlerKey.getEntityType().toCapitalizedString() + " processed since start: " +
        this.attemptedCounter.count() + "; blocked: " + this.blockedCounter.count());
  }
}
