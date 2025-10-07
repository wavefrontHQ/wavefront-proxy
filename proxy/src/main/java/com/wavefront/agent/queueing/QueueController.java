package com.wavefront.agent.queueing;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.common.Managed;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.Pair;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
  private static final double MIN_ADJ_FACTOR = 0.25d;
  private static final double MAX_ADJ_FACTOR = 1.5d;
  private static ScheduledExecutorService executor;

  protected final HandlerKey handlerKey;
  protected final List<QueueProcessor<T>> processorTasks;
  protected final Supplier<Long> timeProvider;
  private final Consumer<Integer> backlogSizeSink;

  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final AtomicLong currentWeight = new AtomicLong();
  private final AtomicInteger queueSize = new AtomicInteger();

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
            return queueSize.get();
          }
        });
    Metrics.newGauge(
        new TaggedMetricName(
            "buffer", handlerKey.getEntityType() + "-count", "port", handlerKey.getHandle()),
        new Gauge<Long>() {
          @Override
          public Long value() {
            return currentWeight.get();
          }
        });
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

  @Override
  public void run() {
    // 1. grab current queue sizes (tasks count) and report to EntityProperties
    int backlog = processorTasks.stream().mapToInt(x -> x.getTaskQueue().size()).sum();
    if (backlogSizeSink != null) {
      backlogSizeSink.accept(backlog);
    }
    // 2. adjust timing
    adjustTimingFactors(processorTasks);
  }

  private void printQueueStats() {
    // 1. grab current queue sizes (tasks count)
    int backlog = processorTasks.stream().mapToInt(x -> x.getTaskQueue().size()).sum();
    queueSize.set(backlog);

    // 2. grab queue sizes (points/etc count)
    long actualWeight = 0L;
    if (backlog > 0) {
      for (QueueProcessor<T> task : processorTasks) {
        TaskQueue<T> taskQueue = task.getTaskQueue();
        if ((taskQueue != null) && (taskQueue.weight() != null)) {
          actualWeight += taskQueue.weight();
        }
      }
    }

    long previousWeight = currentWeight.getAndSet(actualWeight);

    // 4. print stats when there's backlog
    if ((previousWeight != 0) || (actualWeight != 0)) {
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
      if (actualWeight == 0) {
        logger.info(
            "["
                + handlerKey.getHandle()
                + "] "
                + handlerKey.getEntityType()
                + " backlog has been cleared!");
      }
    }
  }

  @Override
  public void start() {
    if (isRunning.compareAndSet(false, true)) {
      if ((executor == null) || (executor.isShutdown())) { // need it for unittests
        executor = Executors.newScheduledThreadPool(2, new NamedThreadFactory("QueueController"));
      }
      executor.scheduleAtFixedRate(this::run, 1, 1, TimeUnit.SECONDS);
      executor.scheduleAtFixedRate(this::printQueueStats, 10, 10, TimeUnit.SECONDS);
      processorTasks.forEach(QueueProcessor::start);
    }
  }

  @Override
  public void stop() {
    if (isRunning.compareAndSet(true, false)) {
      executor.shutdown();
      processorTasks.forEach(QueueProcessor::stop);
    }
  }

  public void truncateBuffers() {
    processorTasks.forEach(
        tQueueProcessor -> {
          logger.info(
              "["
                  + handlerKey.getHandle()
                  + "] "
                  + handlerKey.getEntityType()
                  + "-- size before truncate: "
                  + tQueueProcessor.getTaskQueue().size());
          try {
            tQueueProcessor.getTaskQueue().clear();
          } catch (IOException e) {
            e.printStackTrace();
          }
          logger.info(
              "["
                  + handlerKey.getHandle()
                  + "] "
                  + handlerKey.getEntityType()
                  + "--> size after truncate: "
                  + tQueueProcessor.getTaskQueue().size());
        });
  }
}
