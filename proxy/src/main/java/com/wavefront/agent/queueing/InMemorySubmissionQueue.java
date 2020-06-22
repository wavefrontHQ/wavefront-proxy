package com.wavefront.agent.queueing;

import com.squareup.tape2.ObjectQueue;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements proxy-specific in-memory-queue interface as a wrapper over tape {@link ObjectQueue}
 *
 * @param <T> type of objects stored.
 *
 * @author mike@wavefront.com
 */
public class InMemorySubmissionQueue<T extends DataSubmissionTask<T>> implements TaskQueue<T> {

  private static final Logger log = Logger.getLogger(InMemorySubmissionQueue.class.getCanonicalName());

  private final ObjectQueue<T> delegate;
  @Nullable
  private final String handle;
  private final String entityName;
  private final int maxBufferSize = 50_000;
  private final ReentrantLock queueLock = new ReentrantLock(true);

  private final Counter tasksAddedCounter;
  private final Counter itemsAddedCounter;
  private final Counter tasksRemovedCounter;
  private final Counter itemsRemovedCounter;

  private AtomicLong currentWeight = null;
  private T head;

  /**
   * @param handle       pipeline handle.
   * @param entityType   entity type.
   */
  public InMemorySubmissionQueue(@Nullable String handle,
                                 @Nullable ReportableEntityType entityType) {
    delegate = ObjectQueue.createInMemory();
    this.handle = handle;
    this.entityName = entityType == null ? "points" : entityType.toString();
    if (delegate.isEmpty()) {
      initializeTracking();
    }
    this.tasksAddedCounter = Metrics.newCounter(new TaggedMetricName("buffer.in-memory", "task-added",
        "port", handle));
    this.itemsAddedCounter = Metrics.newCounter(new TaggedMetricName("buffer.in-memory", entityName +
        "-added", "port", handle));
    this.tasksRemovedCounter = Metrics.newCounter(new TaggedMetricName("buffer.in-memory", "task-removed",
        "port", handle));
    this.itemsRemovedCounter = Metrics.newCounter(new TaggedMetricName("buffer.in-memory", entityName +
        "-removed", "port", handle));
  }

  @Override
  public int size() {
    return this.delegate.size();
  }

  @Nullable
  @Override
  public Long weight() {
    return currentWeight != null ? currentWeight.get() : null;
  }

  @Nullable
  @Override
  public Long getAvailableBytes() {
    return null;
  }

  @Nullable
  @Override
  public T peek() {
    if (this.head != null) return this.head;
    queueLock.lock();
    try {
      this.head = delegate.peek();
      return this.head;
    } catch (IOException e) {
      Metrics.newCounter(new TaggedMetricName("buffer.in-memory", "failures", "port", handle)).inc();
      log.log(Level.SEVERE, "I/O error retrieving data from the queue: ", e);
      this.head = null;
      return null;
    } finally {
      queueLock.unlock();
    }
  }

  @Override
  public void add(T t) throws IOException {
    queueLock.lock();
    try {
      if (delegate.size() >= maxBufferSize) {
        log.severe("Unable to enqueue in memory, too many outstanding tasks.");
        return;
      }
      this.delegate.add(t);
      if (currentWeight != null) {
        currentWeight.addAndGet(t.weight());
      }
      tasksAddedCounter.inc();
      itemsAddedCounter.inc(t.weight());
    } finally {
      queueLock.unlock();
    }
  }

  @Override
  public void clear() {
    queueLock.lock();
    try {
      delegate.clear();
      this.head = null;
      initializeTracking();
    } catch (IOException e) {
      Metrics.newCounter(new TaggedMetricName("buffer.in-memory", "failures", "port", handle)).inc();
      log.severe("I/O error clearing queue: " + e.getMessage());
    } finally {
      queueLock.unlock();
    }
  }

  @Override
  public void remove() {
    queueLock.lock();
    long taskSize = head == null ? 0 : head.weight();
    try {
      delegate.remove();
      if (currentWeight != null) {
        currentWeight.getAndUpdate(x -> x > taskSize ? x - taskSize : 0);
      }
      head = null;
      if (delegate.isEmpty()) {
        initializeTracking();
      }
      tasksRemovedCounter.inc();
      itemsRemovedCounter.inc(taskSize);
    } catch (IOException e) {
      Metrics.newCounter(new TaggedMetricName("buffer.in-memory", "failures", "port", handle)).inc();
      log.severe("I/O error removing task from the queue: " + e.getMessage());
    } finally {
      queueLock.unlock();
    }
  }




  @Override
  public void close() {
    try {
      delegate.close();
    } catch (IOException e) {
      Metrics.newCounter(new TaggedMetricName("buffer.in-memory", "failures", "port", handle)).inc();
      log.severe("I/O error closing queue: " + e.getMessage());
    }
  }

  private synchronized void initializeTracking() {
    if (currentWeight == null) {
      currentWeight = new AtomicLong(0);
    } else {
      currentWeight.set(0);
    }
  }
}
