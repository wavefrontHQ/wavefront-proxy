package com.wavefront.agent.queueing;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A thread-safe wrapper for {@link QueueFile}. This version assumes that operations on the head
 * and on the tail of the queue are mutually exclusive and should be synchronized. For a more
 * fine-grained  implementation, see {@link ConcurrentShardedQueueFile} that maintains separate
 * locks on the head and the tail of the queue.
 *
 * @author vasily@wavefront.com
 */
public class ConcurrentQueueFile implements QueueFile {

  private final QueueFile delegate;
  private final ReentrantLock lock = new ReentrantLock(true);

  public ConcurrentQueueFile(QueueFile delegate) {
    this.delegate = delegate;
  }

  @Override
  public void add(byte[] data, int offset, int count) throws IOException {
    lock.lock();
    try {
      delegate.add(data, offset, count);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void clear() throws IOException {
    lock.lock();
    try {
      delegate.clear();
    } finally {
      lock.unlock();
    }
  }

  @Nullable
  @Override
  public byte[] peek() throws IOException {
    lock.lock();
    try {
      return delegate.peek();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void remove() throws IOException {
    lock.lock();
    try {
      delegate.remove();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public long storageBytes() {
    return delegate.storageBytes();
  }

  @Override
  public long usedBytes() {
    return delegate.usedBytes();
  }

  @Override
  public long availableBytes() {
    return delegate.availableBytes();
  }

  @Override
  public void close() throws IOException {
    lock.lock();
    try {
      delegate.close();
    } finally {
      lock.unlock();
    }
  }

  @NotNull
  @Override
  public Iterator<byte[]> iterator() {
    return delegate.iterator();
  }
}
