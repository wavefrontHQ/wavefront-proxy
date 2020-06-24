package com.wavefront.agent.queueing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.jetbrains.annotations.NotNull;

import com.squareup.tape2.ObjectQueue;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.Utils;

/**
 * Implements proxy-specific in-memory-queue interface as a wrapper over tape {@link ObjectQueue}
 *
 * @param <T> type of objects stored.
 *
 * @author mike@wavefront.com
 */
public class InMemorySubmissionQueue<T extends DataSubmissionTask<T>> implements TaskQueue<T> {
  private static final Logger log =
      Logger.getLogger(InMemorySubmissionQueue.class.getCanonicalName());
  private static final int MAX_BUFFER_SIZE = 50_000;

  private final ObjectQueue<T> wrapped;

  private final AtomicLong currentWeight = new AtomicLong();
  private T head;

  public InMemorySubmissionQueue() {
    this.wrapped = ObjectQueue.createInMemory();
  }

  @Override
  public int size() {
    return wrapped.size();
  }

  @Nullable
  @Override
  public Long weight() {
    return currentWeight.get();
  }

  @Nullable
  @Override
  public Long getAvailableBytes() {
    return null;
  }

  @Override
  public String getName() {
    return "unknown";
  }

  @Nullable
  @Override
  public T peek() {
    try {
      if (this.head != null) return this.head;
      this.head = wrapped.peek();
      return this.head;
    } catch (IOException ex) {
      throw Utils.<Error>throwAny(ex);
    }
  }

  @Override
  public void add(@Nonnull T entry) throws IOException {
    if (wrapped.size() >= MAX_BUFFER_SIZE) {
      log.severe("Memory buffer full - too many outstanding tasks (" + MAX_BUFFER_SIZE + ")");
      return;
    }
    wrapped.add(entry);
    currentWeight.addAndGet(entry.weight());
  }

  @Override
  public void clear() throws IOException {
    wrapped.clear();
    this.head = null;
    this.currentWeight.set(0);
  }

  @Override
  public void remove() throws IOException {
    T t = peek();
    long weight = t == null ? 0 : t.weight();
    currentWeight.getAndUpdate(x -> x > weight ? x - weight : 0);
    wrapped.remove();
    head = null;
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }

  @NotNull
  @Override
  public Iterator<T> iterator() {
    return wrapped.iterator();
  }
}
