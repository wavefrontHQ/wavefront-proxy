package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;

import com.wavefront.agent.SharedMetricsRegistry;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 * Base class for all {@link ReportableEntityHandler} implementations.
 *
 * @author vasily@wavefront.com
 *
 * @param <T> the type of input objects handled
 */
abstract class AbstractReportableEntityHandler<T> implements ReportableEntityHandler<T> {
  private static final Logger logger = Logger.getLogger(AbstractReportableEntityHandler.class.getCanonicalName());

  @SuppressWarnings("unchecked")
  private Class<T> type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass())
      .getActualTypeArguments()[0];

  private static SharedMetricsRegistry metricsRegistry = SharedMetricsRegistry.getInstance();

  ScheduledExecutorService statisticOutputExecutor = Executors.newSingleThreadScheduledExecutor();
  private final Logger blockedItemsLogger;

  String handle;
  Counter receivedCounter;
  Counter blockedCounter;
  Counter rejectedCounter;
  final RateLimiter blockedItemsLimiter;
  final Function<T, String> serializer;
  List<SenderTask<T>> senderTasks;

  final Meter receivedMeter;
  private final Histogram receivedBurstRateHistogram;
  private long receivedPrevious;
  long receivedBurstRateCurrent;

  Function<Object, String> serializerFunc;

  private final AtomicLong roundRobinCounter = new AtomicLong();

  /**
   * Base constructor.
   *
   * @param entityType           entity type that dictates the data flow.
   * @param handle               handle (usually port number), that serves as an identifier for the metrics pipeline.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into the main log file.
   * @param serializer           helper function to convert objects to string. Used when writing blocked points to logs.
   * @param senderTasks          tasks actually handling data transfer to the Wavefront endpoint.
   */
  @SuppressWarnings("unchecked")
  AbstractReportableEntityHandler(ReportableEntityType entityType,
                                  @NotNull String handle,
                                  final int blockedItemsPerBatch,
                                  Function<T, String> serializer,
                                  @NotNull Collection<SenderTask> senderTasks) {
    String strEntityType = entityType.toString();
    this.blockedItemsLogger = Logger.getLogger("RawBlocked" + strEntityType.substring(0, 1).toUpperCase() +
        strEntityType.substring(1));
    this.handle = handle;
    this.receivedCounter = Metrics.newCounter(new MetricName(strEntityType + "." + handle, "", "received"));
    this.blockedCounter = Metrics.newCounter(new MetricName(strEntityType + "." + handle, "", "blocked"));
    this.rejectedCounter = Metrics.newCounter(new MetricName(strEntityType + "." + handle, "", "rejected"));
    this.blockedItemsLimiter = blockedItemsPerBatch == 0 ? null : RateLimiter.create(blockedItemsPerBatch / 10d);
    this.serializer = serializer;
    this.serializerFunc = obj -> {
      if (type.isInstance(obj)) {
        return serializer.apply(type.cast(obj));
      } else {
        return null;
      }
    };
    this.senderTasks = new ArrayList<>();
    for (SenderTask task : senderTasks) {
      this.senderTasks.add((SenderTask<T>) task);
    }
    this.receivedMeter = metricsRegistry.newMeter(AbstractReportableEntityHandler.class,
        "received-" + strEntityType + "." + handle, strEntityType, TimeUnit.SECONDS);
    this.receivedBurstRateHistogram = metricsRegistry.newHistogram(AbstractReportableEntityHandler.class,
        "received-" + strEntityType + ".burst-rate." + handle);
    Metrics.newGauge(new MetricName(strEntityType + "." + handle + ".received", "",
        "max-burst-rate"), new Gauge<Double>() {
      @Override
      public Double value() {
        Double maxValue = receivedBurstRateHistogram.max();
        receivedBurstRateHistogram.clear();
        return maxValue;
      }
    });

    this.receivedPrevious = 0;
    this.receivedBurstRateCurrent = 0;
    statisticOutputExecutor.scheduleAtFixedRate(() -> {
      long received = this.receivedCounter.count();
      this.receivedBurstRateCurrent = received - this.receivedPrevious;
      this.receivedBurstRateHistogram.update(this.receivedBurstRateCurrent);
      this.receivedPrevious = received;
    }, 1, 1, TimeUnit.SECONDS);
  }

  @Override
  public void reject(T item) {
    blockedCounter.inc();
    rejectedCounter.inc();
    if (item != null) {
      blockedItemsLogger.warning(serializer.apply(item));
    }
    if (blockedItemsLimiter != null && blockedItemsLimiter.tryAcquire()) {
      logger.info("[" + handle + "] blocked input: [" + serializer.apply(item) + "]");
    }
  }

  @Override
  public void reject(@Nullable T item, @Nullable String message) {
    blockedCounter.inc();
    rejectedCounter.inc();
    if (item != null) {
      blockedItemsLogger.warning(serializer.apply(item));
    }
    if (message != null && blockedItemsLimiter != null && blockedItemsLimiter.tryAcquire()) {
      logger.info("[" + handle + "] blocked input: [" + message + "]");
    }
  }

  @Override
  public void reject(@Nullable String line, @Nullable String message) {
    blockedCounter.inc();
    rejectedCounter.inc();
    if (line != null) {
      blockedItemsLogger.warning(line);
    }
    if (message != null && blockedItemsLimiter != null && blockedItemsLimiter.tryAcquire()) {
      logger.info("[" + handle + "] blocked input: [" + message + "]");
    }
  }

  @Override
  public void block(T item) {
    blockedCounter.inc();
    blockedItemsLogger.info(serializer.apply(item));
  }

  @Override
  public void block(@Nullable T item, @Nullable String message) {
    blockedCounter.inc();
    if (item != null) {
      blockedItemsLogger.info(serializer.apply(item));
    }
    if (message != null) {
      blockedItemsLogger.info(message);
    }
  }

  @Override
  public void report(T item) {
    report(item, item, serializerFunc);
  }

  @Override
  public void report(T item, @Nullable Object messageObject,
                     @NotNull Function<Object, String> messageSerializer) {
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

  protected SenderTask getTask() {
    return senderTasks.get((int)(roundRobinCounter.getAndIncrement() % senderTasks.size()));
  }
  /*
  protected SenderTask getTask() {
    SenderTask toReturn = null;
    long bestScore = Long.MAX_VALUE;
    for (SenderTask task : senderTasks) {
      long score = task.getTaskRelativeScore();
      if (score < bestScore) {
        bestScore = score;
        toReturn = task;
      }
    }
    return toReturn;
  }*/
}
