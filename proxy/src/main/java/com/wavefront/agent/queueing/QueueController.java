package com.wavefront.agent.queueing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.common.Managed;
import com.wavefront.common.Pair;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A queue controller (one per entity/port). Responsible for reporting queue-related metrics and
 * adjusting priority across queues.
 *
 * @param <T> submission task type
 */
public class QueueController<T extends DataSubmissionTask<T>> extends TimerTask implements Managed {
  private static final Logger logger = Logger.getLogger(QueueController.class.getCanonicalName());

  // min difference in queued timestamps for the schedule adjuster to kick in
  private static final int TIME_DIFF_THRESHOLD_SECS = 60;
  private static final int REPORT_QUEUE_STATS_DELAY_SECS = 15;
  private static final double MIN_ADJ_FACTOR = 0.25d;
  private static final double MAX_ADJ_FACTOR = 1.5d;

  protected final HandlerKey handlerKey;
  protected final List<QueueProcessor<T>> processorTasks;
  @Nullable private final Consumer<Integer> backlogSizeSink;
  protected final Supplier<Long> timeProvider;
  protected final Timer timer;

  @SuppressWarnings("UnstableApiUsage")
  protected final RateLimiter reportRateLimiter = RateLimiter.create(0.1);

  private long currentWeight;
  private int queueSize;

  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  /**
   * @param handlerKey Pipeline handler key
   * @param processorTasks List of {@link QueueProcessor} tasks responsible for processing the
   *     backlog.
   * @param backlogSizeSink Where to report backlog size.
   */
  public QueueController(
      HandlerKey handlerKey,
      List<QueueProcessor<T>> processorTasks,
      @Nullable Consumer<Integer> backlogSizeSink) {
    this(handlerKey, processorTasks, backlogSizeSink, System::currentTimeMillis);
  }

  /**
   * @param handlerKey Pipeline handler key
   * @param processorTasks List of {@link QueueProcessor} tasks responsible for processing the
   *     backlog.
   * @param backlogSizeSink Where to report backlog size.
   * @param timeProvider current time provider (in millis).
   */
  QueueController(
      HandlerKey handlerKey,
      List<QueueProcessor<T>> processorTasks,
      @Nullable Consumer<Integer> backlogSizeSink,
      Supplier<Long> timeProvider) {
    this.handlerKey = handlerKey;
    this.processorTasks = processorTasks;
    this.backlogSizeSink = backlogSizeSink;
    this.timeProvider = timeProvider == null ? System::currentTimeMillis : timeProvider;
    this.timer = new Timer("timer-queuedservice-" + handlerKey.toString());

    Metrics.newGauge(
        new TaggedMetricName(
            "buffer",
            "task-count",
            "port",
            handlerKey.getHandle(),
            "content",
            handlerKey.getEntityType().toString()),
        new Gauge<Integer>() {
          @Override
          public Integer value() {
            return queueSize;
          }
        });
    Metrics.newGauge(
        new TaggedMetricName(
            "buffer", handlerKey.getEntityType() + "-count", "port", handlerKey.getHandle()),
        new Gauge<Long>() {
          @Override
          public Long value() {
            return currentWeight;
          }
        });
  }

  @Override
  public void run() {
    // 1. grab current queue sizes (tasks count) and report to EntityProperties
    int backlog = processorTasks.stream().mapToInt(x -> x.getTaskQueue().size()).sum();
    queueSize = backlog;
    if (backlogSizeSink != null) {
      backlogSizeSink.accept(backlog);
    }

    // 2. grab queue sizes (points/etc count)
    long totalWeight = 0L;
    for (QueueProcessor<T> task : processorTasks) {
      TaskQueue<T> taskQueue = task.getTaskQueue();
      if ((taskQueue != null) && (taskQueue.weight() != null)) {
        totalWeight += taskQueue.weight();
      }
    }
    long previousWeight = currentWeight;
    currentWeight = totalWeight;

    // 3. adjust timing
    adjustTimingFactors(processorTasks);

    // 4. print stats when there's backlog
    if ((previousWeight != 0) || (currentWeight != 0)) {
      printQueueStats();
      if (currentWeight == 0) {
        logger.info(
            "["
                + handlerKey.getHandle()
                + "] "
                + handlerKey.getEntityType()
                + " backlog has been cleared!");
      }
    }
  }

  /**
   * Compares timestamps of tasks at the head of all backing queues. If the time difference between
   * most recently queued head and the oldest queued head (across all backing queues) is less than
   * {@code TIME_DIFF_THRESHOLD_SECS}, restore timing factor to 1.0d for all processors. If the
   * difference is higher, adjust timing factors proportionally (use linear interpolation to stretch
   * timing factor between {@code MIN_ADJ_FACTOR} and {@code MAX_ADJ_FACTOR}.
   *
   * @param processors processors
   */
  @VisibleForTesting
  static <T extends DataSubmissionTask<T>> void adjustTimingFactors(
      List<QueueProcessor<T>> processors) {
    List<Pair<QueueProcessor<T>, Long>> sortedProcessors =
        processors.stream()
            .map(x -> new Pair<>(x, x.getHeadTaskTimestamp()))
            .filter(x -> x._2 < Long.MAX_VALUE)
            .sorted(Comparator.comparing(o -> o._2))
            .collect(Collectors.toList());
    if (sortedProcessors.size() > 1) {
      long minTs = sortedProcessors.get(0)._2;
      long maxTs = sortedProcessors.get(sortedProcessors.size() - 1)._2;
      if (maxTs - minTs > TIME_DIFF_THRESHOLD_SECS * 1000) {
        sortedProcessors.forEach(
            x ->
                x._1.setTimingFactor(
                    MIN_ADJ_FACTOR
                        + ((double) (x._2 - minTs) / (maxTs - minTs))
                            * (MAX_ADJ_FACTOR - MIN_ADJ_FACTOR)));
      } else {
        processors.forEach(x -> x.setTimingFactor(1.0d));
      }
    }
  }

  private void printQueueStats() {
    long oldestTaskTimestamp =
        processorTasks.stream()
            .filter(x -> x.getTaskQueue().size() > 0)
            .mapToLong(QueueProcessor::getHeadTaskTimestamp)
            .min()
            .orElse(Long.MAX_VALUE);
    //noinspection UnstableApiUsage
    if ((oldestTaskTimestamp < timeProvider.get() - REPORT_QUEUE_STATS_DELAY_SECS * 1000)
        && (reportRateLimiter.tryAcquire())) {
      logger.info(
          "["
              + handlerKey.getHandle()
              + "] "
              + handlerKey.getEntityType()
              + " backlog status: "
              + queueSize
              + " tasks, "
              + currentWeight
              + " "
              + handlerKey.getEntityType());
    }
  }

  @Override
  public void start() {
    if (isRunning.compareAndSet(false, true)) {
      timer.scheduleAtFixedRate(this, 1000, 1000);
      processorTasks.forEach(QueueProcessor::start);
    }
  }

  @Override
  public void stop() {
    if (isRunning.compareAndSet(true, false)) {
      timer.cancel();
      processorTasks.forEach(QueueProcessor::stop);
    }
  }

  public void truncateBuffers() {
    processorTasks.forEach(
        tQueueProcessor -> {
          System.out.print("-- size: " + tQueueProcessor.getTaskQueue().size());
          try {
            tQueueProcessor.getTaskQueue().clear();
          } catch (IOException e) {
            e.printStackTrace();
          }
          System.out.println("--> size: " + tQueueProcessor.getTaskQueue().size());
        });
  }
}
