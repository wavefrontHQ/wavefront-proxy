package com.wavefront.agent.queueing;

import com.wavefront.agent.data.DataSubmissionTask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.jetbrains.annotations.NotNull;

/**
 * A non-functional empty {@code TaskQueue} that throws an error when attempting to add a task.
 * To be used as a stub when dynamic provisioning of queues failed.
 *
 * @author vasily@wavefront.com
 */
public class TaskQueueStub<T extends DataSubmissionTask<T>> implements TaskQueue<T>{

  @Override
  public T peek() {
    return null;
  }

  @Override
  public void add(@Nonnull T t) throws IOException {
    throw new IOException("Storage queue is not available!");
  }

  @Override
  public void remove() {
  }

  @Override
  public void clear() {
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public void close() {
  }

  @Nullable
  @Override
  public Long weight() {
    return null;
  }

  @Nullable
  @Override
  public Long getAvailableBytes() {
    return null;
  }

  @Override
  public String getName() {
    return "stub";
  }

  @NotNull
  @Override
  public Iterator<T> iterator() {
    return EmptyIterator.INSTANCE;
  }
}
