package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.BurstTrackingCounter;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base class for all {@link ReportableEntityHandler} implementations.
 *
 * @author vasily@wavefront.com
 *
 * @param <T> the type of input objects handled
 */
abstract class AbstractReportableEntityHandler<T, U> implements ReportableEntityHandler<T, U> {
  private static final Logger logger = Logger.getLogger(
      AbstractReportableEntityHandler.class.getCanonicalName());

  private final Class<T> type;
  private final Logger blockedItemsLogger;

  final ReportableEntityType entityType;
  final String handle;
  private final Counter receivedCounter;
  private final Counter attemptedCounter;
  private final Counter blockedCounter;
  private final Counter rejectedCounter;

  @SuppressWarnings("UnstableApiUsage")
  final RateLimiter blockedItemsLimiter;
  final Function<T, String> serializer;
  final List<SenderTask<U>> senderTasks;
  final Supplier<ValidationConfiguration> validationConfig;
  final String rateUnit;
  final Function<Object, String> serializerFunc;

  final BurstTrackingCounter receivedStats;
  final BurstTrackingCounter deliveredStats;

  private final AtomicLong roundRobinCounter = new AtomicLong();

  /**
   * Base constructor.
   *
   * @param entityType           entity type that dictates the data flow.
   * @param handle               handle (usually port number), that serves as an identifier
   *                             for the metrics pipeline.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written
   *                             into the main log file.
   * @param serializer           helper function to convert objects to string. Used when writing
   *                             blocked points to logs.
   * @param senderTasks          tasks actually handling data transfer to the Wavefront endpoint.
   * @param validationConfig     supplier for the validation configuration.
   * @param rateUnit             optional display name for unit of measure. Default: rps
   * @param setupMetrics         Whether we should report counter metrics.
   * @param blockedItemsLogger   a {@link Logger} instance for blocked items
   */
  AbstractReportableEntityHandler(ReportableEntityType entityType,
                                  @Nonnull String handle,
                                  final int blockedItemsPerBatch,
                                  Function<T, String> serializer,
                                  @Nullable Collection<SenderTask<U>> senderTasks,
                                  @Nullable Supplier<ValidationConfiguration> validationConfig,
                                  @Nullable String rateUnit,
                                  boolean setupMetrics,
                                  @Nullable final Logger blockedItemsLogger) {
    this.entityType = entityType;
    this.blockedItemsLogger = blockedItemsLogger;
    this.handle = handle;
    //noinspection UnstableApiUsage
    this.blockedItemsLimiter = blockedItemsPerBatch == 0 ? null :
        RateLimiter.create(blockedItemsPerBatch / 10d);
    this.serializer = serializer;
    this.type = getType();
    this.serializerFunc = obj -> {
      if (type.isInstance(obj)) {
        return serializer.apply(type.cast(obj));
      } else {
        return null;
      }
    };
    this.senderTasks = senderTasks == null ? new ArrayList<>() : new ArrayList<>(senderTasks);
    this.validationConfig = validationConfig == null ? () -> null : validationConfig;
    this.rateUnit = rateUnit == null ? "rps" : rateUnit;

    MetricsRegistry registry = setupMetrics ? Metrics.defaultRegistry(): new MetricsRegistry();
    String metricPrefix = entityType.toString() + "." + handle;
    MetricName receivedMetricName = new MetricName(metricPrefix, "", "received");
    MetricName deliveredMetricName = new MetricName(metricPrefix, "", "delivered");
    this.receivedCounter = registry.newCounter(receivedMetricName);
    this.attemptedCounter = registry.newCounter(new MetricName(metricPrefix, "", "sent"));
    this.blockedCounter = registry.newCounter(new MetricName(metricPrefix, "", "blocked"));
    this.rejectedCounter = registry.newCounter(new MetricName(metricPrefix, "", "rejected"));
    this.receivedStats = new BurstTrackingCounter(receivedMetricName);
    this.deliveredStats = new BurstTrackingCounter(deliveredMetricName);
    Metrics.newGauge(new MetricName(entityType.toString() + "." + handle + ".received", "",
        "max-burst-rate"), new Gauge<Double>() {
      @Override
      public Double value() {
        return receivedStats.getMaxBurstRateAndClear();
      }
    });
    if (setupMetrics) {
      ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();
      statsExecutor.scheduleAtFixedRate(this::printStats, 10, 10, TimeUnit.SECONDS);
      statsExecutor.scheduleAtFixedRate(this::printTotal, 1, 1, TimeUnit.MINUTES);
    }
  }

  @Override
  public void reject(T item) {
    blockedCounter.inc();
    rejectedCounter.inc();
    if (item != null && blockedItemsLogger != null) {
      blockedItemsLogger.warning(serializer.apply(item));
    }
    //noinspection UnstableApiUsage
    if (blockedItemsLimiter != null && blockedItemsLimiter.tryAcquire()) {
      logger.info("[" + handle + "] blocked input: [" + serializer.apply(item) + "]");
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
      logger.info("[" + handle + "] blocked input: [" + message + "]");
    }
  }

  @Override
  public void reject(@Nonnull String line, @Nullable String message) {
    blockedCounter.inc();
    rejectedCounter.inc();
    if (blockedItemsLogger != null) blockedItemsLogger.warning(line);
    //noinspection UnstableApiUsage
    if (message != null && blockedItemsLimiter != null && blockedItemsLimiter.tryAcquire()) {
      logger.info("[" + handle + "] blocked input: [" + message + "]");
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
    report(item, item, serializerFunc);
  }

  @Override
  public void report(T item, @Nullable Object messageObject,
                     @Nonnull Function<Object, String> messageSerializer) {
    try {
      reportInternal(item);
    } catch (IllegalArgumentException e) {
      this.reject(item, e.getMessage() + " (" + messageSerializer.apply(messageObject) + ")");
    } catch (Exception ex) {
      logger.log(Level.SEVERE, "WF-500 Uncaught exception when handling input (" +
          messageSerializer.apply(messageObject) + ")", ex);
    }
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
    logger.info("[" + this.handle + "] " + entityType.toCapitalizedString() + " received rate: " +
        receivedStats.getOneMinutePrintableRate() + " " + rateUnit + " (1 min), " +
        receivedStats.getFiveMinutePrintableRate() + " " + rateUnit + " (5 min), " +
        receivedStats.getCurrentRate() + " " + rateUnit + " (current).");
    if (deliveredStats.getFiveMinuteCount() == 0) return;
    logger.info("[" + this.handle + "] " + entityType.toCapitalizedString() + " delivered rate: " +
        deliveredStats.getOneMinutePrintableRate() + " " + rateUnit + " (1 min), " +
        deliveredStats.getFiveMinutePrintableRate() + " " + rateUnit + " (5 min)");
    // we are not going to display current delivered rate because it _will_ be misinterpreted.
  }

  protected void printTotal() {
    logger.info("[" + this.handle + "] Total " + entityType.toString() +
        " processed since start: " + this.attemptedCounter.count() + "; blocked: " +
        this.blockedCounter.count());
  }

  private Class<T> getType() {
    Type type = getClass().getGenericSuperclass();
    ParameterizedType parameterizedType = null;
    while (parameterizedType == null) {
      if (type instanceof ParameterizedType) {
        parameterizedType = (ParameterizedType) type;
      } else {
        type = ((Class<?>) type).getGenericSuperclass();
      }
    }
    //noinspection unchecked
    return (Class<T>) parameterizedType.getActualTypeArguments()[0];
  }
}
