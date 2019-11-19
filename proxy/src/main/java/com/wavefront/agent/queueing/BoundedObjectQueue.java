package com.wavefront.agent.queueing;

import com.squareup.tape2.ObjectQueue;
import com.squareup.tape2.QueueFile;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.RejectedExecutionException;

/**
 * A queue that has a hard limit on the number of items in it. Essential for in-memory queues to prevent
 * uncontrollable queue growth and eventual OOMs.
 *
 * @param <T> the type of objects stored in the queue.
 *
 * @author vasily@wavefront.com.
 */
public class BoundedObjectQueue<T> extends ObjectQueue<T> {

  private final ObjectQueue<T> delegate;
  private final Integer maxCapacity;

  public BoundedObjectQueue(ObjectQueue<T> delegate, @Nullable Integer maxCapacity) {
    super();
    this.delegate = delegate;
    this.maxCapacity = maxCapacity;
  }

  public int availableCapacity() {
    if (maxCapacity == null) {
      return Integer.MAX_VALUE;
    }
    int available = maxCapacity - delegate.size();
    return available <= 0 ? 0 : available;
  }

  public boolean isFull() {
    return availableCapacity() == 0;
  }

  @Nullable
  @Override
  public QueueFile file() {
    return delegate.file();
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public void add(@Nonnull T t) throws IOException, RejectedExecutionException {
    if (maxCapacity != null && delegate.size() >= maxCapacity) {
      throw new RejectedExecutionException("Queue has reached its capacity.");
    }
    delegate.add(t);
  }

  @Nullable
  @Override
  public T peek() throws IOException {
    return delegate.peek();
  }

  @Override
  public void remove(int i) throws IOException {
    delegate.remove(i);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Nonnull
  @Override
  public Iterator<T> iterator() {
    return delegate.iterator();
  }
}
