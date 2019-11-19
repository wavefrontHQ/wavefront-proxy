package com.wavefront.agent.data;

import avro.shaded.com.google.common.base.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.TimerContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 *
 *
 * @param <T>
 *
 * @author vasily@wavefront.com
 */
abstract class AbstractDataSubmissionTask<T extends DataSubmissionTask<T>>
    implements DataSubmissionTask<T> {
  // to ensure backwards compatibility
  private static final long serialVersionUID = 1973695079812309903L;

  @JsonProperty
  private final Long createdMillis;
  @JsonProperty
  protected Long enqueuedTimeMillis = null;
  @JsonProperty
  private int attempts = 0;
  @JsonProperty
  protected String handle;
  @JsonProperty
  protected ReportableEntityType entityType;

  private transient Histogram timeSpentInQueue;
  protected transient Supplier<Long> timeProvider;

  /**
   *
   *
   *
   * @param handle
   * @param entityType
   * @param timeProvider
   */
  AbstractDataSubmissionTask(String handle, ReportableEntityType entityType,
                             @Nullable Supplier<Long> timeProvider) {
    this.handle = handle;
    this.entityType = entityType;
    this.timeProvider = Objects.firstNonNull(timeProvider, System::currentTimeMillis);
    this.createdMillis = this.timeProvider.get();
  }

  @Override
  public int getAttempts() {
    return attempts;
  }

  @Override
  public long getCreatedMillis() {
    return createdMillis;
  }

  @Override
  public ReportableEntityType getEntityType() {
    return entityType;
  }

  abstract TaskResult doExecute(TaskQueueingDirective queuingLevel,
                                TaskQueue<T> taskQueue);

  @Override
  public TaskResult execute(TaskQueueingDirective queueingLevel,
                            TaskQueue<T> backlog) {
    if (enqueuedTimeMillis != null) {
      if (timeSpentInQueue == null) {
        timeSpentInQueue = Metrics.newHistogram(new TaggedMetricName("buffer", "queue-time",
            "port", handle, "content", entityType.toString()));
      }
      timeSpentInQueue.update(timeProvider.get() - enqueuedTimeMillis);
    }
    attempts += 1;
    TimerContext timer = Metrics.newTimer(new MetricName("push." + handle, "", "duration"),
        TimeUnit.MILLISECONDS, TimeUnit.MINUTES).time();
    try {
      return doExecute(queueingLevel, backlog);
    } finally {
      timer.stop();
    }
  }

  @Override
  public void enqueue(TaskQueue<T> taskQueue) throws IOException {
    enqueuedTimeMillis = timeProvider.get();
    taskQueue.add((T) this);
  }
}
