package com.wavefront.agent.queueing;

import com.google.common.collect.ImmutableList;
import com.squareup.tape2.ObjectQueue;
import com.squareup.tape2.QueueFile;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * Implements proxy-specific queue interface as a wrapper over tape {@link ObjectQueue}
 *
 * @param <T> type of objects stored.
 *
 * @author vasily@wavefront.com
 */
public class DataSubmissionQueue<T extends DataSubmissionTask<T>> extends ObjectQueue<T>
    implements TaskQueue<T> {
  private static final Logger log = Logger.getLogger(DataSubmissionQueue.class.getCanonicalName());

  private static final int DEFAULT_BATCH_WEIGHT = 50000;

  private final ObjectQueue<T> delegate;

  private AtomicLong currentWeight = null;
  private final AtomicLong lastKnownTaskSize = new AtomicLong(-1);
  @Nullable
  private final String handle;
  private final String entityName;

  // maintain a fair lock on the queue
  private final ReentrantLock queueLock = new ReentrantLock(true);

  /**
   * @param delegate   delegate {@link ObjectQueue}.
   * @param handle     pipeline handle.
   * @param entityType entity type.
   */
  public DataSubmissionQueue(ObjectQueue<T> delegate,
                             @Nullable String handle,
                             @Nullable ReportableEntityType entityType) {
    this.delegate = delegate;
    this.handle = handle;
    this.entityName = entityType == null ? "points" : entityType.toString();
    if (delegate.isEmpty()) {
      initializeTracking();
    }
  }

  @Override
  public QueueFile file() {
    return delegate.file();
  }

  @Override
  public List<T> peek(int max) {
    if (max > 1) {
      throw new UnsupportedOperationException("Cannot peek more than 1 task at a time");
    }
    T t = peek();
    return t == null ? Collections.emptyList() : ImmutableList.of(t);
  }

  @Override
  public T peek() {
    queueLock.lock();
    try {
      T t = delegate.peek();
      lastKnownTaskSize.set(t == null ? 0 : t.weight());
      return t;
    } catch (IOException e) {
      Metrics.newCounter(new TaggedMetricName("buffer", "failures", "port", handle)).inc();
      log.severe("I/O error retrieving data from the queue: " + e.getMessage());
      return null;
    } finally {
      queueLock.unlock();
    }
  }

  @Override
  public void add(@Nonnull T t) throws IOException {
    queueLock.lock();
    try {
      delegate.add(t);
      if (currentWeight != null) {
        currentWeight.addAndGet(t.weight());
      }
    } finally {
      queueLock.unlock();
      Metrics.newCounter(new TaggedMetricName("buffer", "task-added", "port", handle)).inc();
      Metrics.newCounter(new TaggedMetricName("buffer", entityName + "-added", "port", handle)).
          inc(t.weight());
    }
  }

  @Override
  public void clear() {
    queueLock.lock();
    try {
      delegate.clear();
      initializeTracking();
    } catch (IOException e) {
      Metrics.newCounter(new TaggedMetricName("buffer", "failures", "port", handle)).inc();
      log.severe("I/O error clearing queue: " + e.getMessage());
    } finally {
      queueLock.unlock();
    }
  }

  @Override
  public void remove(int tasksToRemove) {
    if (tasksToRemove > 1) {
      throw new UnsupportedOperationException("Cannot remove more than 1 task at a time");
    }
    queueLock.lock();
    long taskSize = lastKnownTaskSize.get();
    try {
      if (taskSize == -1) {
        throw new IllegalStateException("remove() without peek() is not supported");
      }
      delegate.remove();
      if (currentWeight != null) {
        currentWeight.getAndUpdate(x -> x > taskSize ? x - taskSize : 0);
      }
      lastKnownTaskSize.set(-1);
      if (delegate.isEmpty()) {
        initializeTracking();
      }
    } catch (IOException e) {
      Metrics.newCounter(new TaggedMetricName("buffer", "failures", "port", handle)).inc();
      log.severe("I/O error removing task from the queue: " + e.getMessage());
    } finally {
      queueLock.unlock();
      Metrics.newCounter(new TaggedMetricName("buffer", "task-removed", "port", handle)).inc();
      Metrics.newCounter(new TaggedMetricName("buffer", entityName + "-removed", "port", handle)).
          inc(taskSize);
    }
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public void close() {
    try {
      delegate.close();
    } catch (IOException e) {
      Metrics.newCounter(new TaggedMetricName("buffer", "failures", "port", handle)).inc();
      log.severe("I/O error closing queue: " + e.getMessage());
    }
  }

  @Nonnull
  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException("Iterators are not supported");
  }

  @Nullable
  @Override
  public Long weight() {
    return currentWeight == null ? null : currentWeight.get();
  }

  private synchronized void initializeTracking() {
    if (currentWeight == null) {
      currentWeight = new AtomicLong(0);
    } else {
      currentWeight.set(0);
    }
  }
}
