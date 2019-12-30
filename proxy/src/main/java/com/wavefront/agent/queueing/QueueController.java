package com.wavefront.agent.queueing;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.Managed;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.Pair;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import java.util.Comparator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * A queue controller (one per entity/port). Responsible for reporting queue-related metrics and
 * adjusting priority across queues.
 *
 * @param <T> submission task type
 *
 * @author vasily@wavefront.com
 */
public class QueueController<T extends DataSubmissionTask<T>> extends TimerTask implements Managed {
  private static final Logger logger =
      Logger.getLogger(QueueController.class.getCanonicalName());

  // min difference in queued timestamps for the schedule adjuster to kick in
  private static final int TIME_DIFF_THRESHOLD_SECS = 60;
  private static final double MIN_ADJ_FACTOR = 0.25d;
  private static final double MAX_ADJ_FACTOR = 1.5d;

  protected final String handle;
  protected final ReportableEntityType entityType;
  protected final String entityName;
  protected final List<QueueProcessor<T>> processorTasks;
  protected final Timer timer;

  private AtomicLong currentWeight = null;
  private final AtomicInteger queueSize = new AtomicInteger();
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  /**
   * @param handle         Pipeline handle.
   * @param entityType     Entity type.
   * @param processorTasks List of {@link QueueProcessor} tasks responsible for processing the
   *                       backlog.
   */
  public QueueController(String handle, ReportableEntityType entityType,
                         List<QueueProcessor<T>> processorTasks) {
    this.handle = handle;
    this.entityType = entityType;
    this.entityName = entityType == null ? "points" : entityType.toString();
    this.processorTasks = processorTasks;
    this.timer = new Timer("timer-queuedservice-" + entityName + "-" + handle);

    Metrics.newGauge(new TaggedMetricName("buffer", "task-count", "port", handle),
        new Gauge<Integer>() {
          @Override
          public Integer value() {
            return queueSize.get();
          }
        });
  }

  @Override
  public void run() {
    logger.info(">>> QueueController " + handle + "/" + entityType + " run()");
    // 1. grab current queue sizes (tasks count)
    queueSize.set(processorTasks.stream().mapToInt(x -> x.taskQueue.size()).sum());

    // 2. grab queue sizes (points/etc count)
    Long totalWeight = 0L;
    for (QueueProcessor<T> task : processorTasks) {
      totalWeight = task.taskQueue.weight() == null ? null : totalWeight + task.taskQueue.weight();
      if (totalWeight == null) break;
    }
    if (totalWeight != null) {
      if (currentWeight == null) {
        currentWeight = new AtomicLong();
        Metrics.newGauge(new TaggedMetricName("buffer", entityName + "-count", "port", handle),
            new Gauge<Long>() {
              @Override
              public Long value() {
                return currentWeight.get();
              }
            });
      }
      currentWeight.set(totalWeight);
    }

    adjustTimingFactors(processorTasks);
  }

  @VisibleForTesting
  void adjustTimingFactors(List<QueueProcessor<T>> processors) {
    List<Pair<QueueProcessor<T>, Long>> sortedProcessors = processors.stream().
        map(x -> new Pair<>(x, x.getLastSeenTimestamp())).
        filter(x -> x._2 > Long.MIN_VALUE).
        sorted(Comparator.comparing(o -> o._2)).
        collect(Collectors.toList());
    if (sortedProcessors.size() > 1) {
      long minTs = sortedProcessors.get(0)._2;
      long maxTs = sortedProcessors.get(sortedProcessors.size() - 1)._2;
      if (maxTs - minTs > TIME_DIFF_THRESHOLD_SECS * 1000) {
        sortedProcessors.forEach(x -> x._1.setTimingFactor(MIN_ADJ_FACTOR +
            ((double)(x._2 - minTs) / (maxTs - minTs)) * (MAX_ADJ_FACTOR - MIN_ADJ_FACTOR)));
      } else {
        processorTasks.forEach(x -> x.setTimingFactor(1.0d));
      }
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
}
