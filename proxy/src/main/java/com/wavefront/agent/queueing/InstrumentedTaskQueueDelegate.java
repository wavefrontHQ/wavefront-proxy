package com.wavefront.agent.queueing;

import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * A thread-safe wrapper for {@link TaskQueue<T>} that reports queue metrics.
 *
 * @param <T> type of objects stored.
 *
 * @author vasily@wavefront.com
 */
public class InstrumentedTaskQueueDelegate<T extends DataSubmissionTask<T>> implements TaskQueue<T> {
  private static final Logger log =
      Logger.getLogger(InstrumentedTaskQueueDelegate.class.getCanonicalName());

  private final TaskQueue<T> delegate;
  private volatile T head;

  private final String prefix;
  private final Map<String, String> tags;
  private final Counter tasksAddedCounter;
  private final Counter itemsAddedCounter;
  private final Counter tasksRemovedCounter;
  private final Counter itemsRemovedCounter;

  /**
   * @param delegate     delegate {@link TaskQueue<T>}.
   * @param metricPrefix prefix for metric names (default: "buffer")
   * @param metricTags   point tags for metrics (default: none)
   * @param entityType   entity type (default: points)
   */
  public InstrumentedTaskQueueDelegate(TaskQueue<T> delegate,
                                       @Nullable String metricPrefix,
                                       @Nullable Map<String, String> metricTags,
                                       @Nullable ReportableEntityType entityType) {
    this.delegate = delegate;
    String entityName = entityType == null ? "points" : entityType.toString();
    this.prefix = firstNonNull(metricPrefix, "buffer");
    this.tags = metricTags == null ? ImmutableMap.of() : metricTags;
    this.tasksAddedCounter = Metrics.newCounter(new TaggedMetricName(prefix, "task-added", tags));
    this.itemsAddedCounter = Metrics.newCounter(new TaggedMetricName(prefix, entityName + "-added",
        tags));
    this.tasksRemovedCounter = Metrics.newCounter(new TaggedMetricName(prefix, "task-removed",
        tags));
    this.itemsRemovedCounter = Metrics.newCounter(new TaggedMetricName(prefix, entityName +
        "-removed", tags));
  }

  @Override
  public T peek() {
    try {
      if (this.head != null) return this.head;
      this.head = delegate.peek();
      return this.head;
    } catch (Exception e) {
      //noinspection ConstantConditions
      if (e instanceof IOException) {
        Metrics.newCounter(new TaggedMetricName(prefix, "failures", tags)).inc();
        log.severe("I/O error retrieving data from the queue: " + e.getMessage());
        this.head = null;
        return null;
      } else {
        throw e;
      }
    }
  }

  @Override
  public void add(@Nonnull T t) throws IOException {
    delegate.add(t);
    tasksAddedCounter.inc();
    itemsAddedCounter.inc(t.weight());
  }

  @Override
  public void clear() {
    try {
      this.head = null;
      delegate.clear();
    } catch (IOException e) {
      Metrics.newCounter(new TaggedMetricName(prefix, "failures", tags)).inc();
      log.severe("I/O error clearing queue: " + e.getMessage());
    }
  }

  @Override
  public void remove() {
    try {
      T t = this.head == null ? delegate.peek() : head;
      long size = t == null ? 0 : t.weight();
      delegate.remove();
      head = null;
      tasksRemovedCounter.inc();
      itemsRemovedCounter.inc(size);
    } catch (IOException e) {
      Metrics.newCounter(new TaggedMetricName(prefix, "failures", tags)).inc();
      log.severe("I/O error removing task from the queue: " + e.getMessage());
    }
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Nullable
  @Override
  public Long weight() {
    return delegate.weight();
  }

  @Nullable
  @Override
  public Long getAvailableBytes()  {
    return delegate.getAvailableBytes();
  }

  @Nonnull
  @Override
  public Iterator<T> iterator() {
    return delegate.iterator();
  }
}
