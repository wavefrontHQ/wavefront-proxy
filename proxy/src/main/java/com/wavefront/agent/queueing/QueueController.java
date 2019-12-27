package com.wavefront.agent.queueing;

import com.wavefront.agent.Managed;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

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

  protected final String handle;
  protected final ReportableEntityType entityType;
  protected final String entityName;
  protected final List<QueueProcessor<T>> processorTasks;
  protected final List<Long> lastProcessedTs;
  protected final Timer timer;

  private AtomicLong currentWeight = null;
  private final AtomicInteger queueSize = new AtomicInteger();
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  /**
   * TODO (VV): javadoc
   *
   * @param handle
   * @param entityType
   * @param processorTasks
   */
  public QueueController(String handle, ReportableEntityType entityType,
                         List<QueueProcessor<T>> processorTasks) {
    this.handle = handle;
    this.entityType = entityType;
    this.entityName = entityType == null ? "points" : entityType.toString();
    this.processorTasks = processorTasks;
    this.lastProcessedTs = processorTasks.stream().map(x -> x.lastProcessedTs).
        collect(Collectors.toList());
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

    // TODO (VV): grab last processed ts, adjust scheduler
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
