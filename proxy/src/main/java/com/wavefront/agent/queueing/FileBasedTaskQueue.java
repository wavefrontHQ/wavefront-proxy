package com.wavefront.agent.queueing;

import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Implements proxy-specific {@link TaskQueue<T>} interface as a wrapper over {@link QueueFile}.
 *
 * @param <T> type of objects stored.
 *
 * @author vasily@wavefront.com
 */
public class FileBasedTaskQueue<T extends DataSubmissionTask<T>> implements TaskQueue<T> {
  private static final Logger log = Logger.getLogger(FileBasedTaskQueue.class.getCanonicalName());

  private final DirectByteArrayOutputStream bytes = new DirectByteArrayOutputStream();

  private volatile T head;

  private final AtomicLong currentWeight = new AtomicLong();
  private final QueueFile queueFile;
  private final TaskConverter<T> taskConverter;

  /**
   * @param queueFile     file backing the queue
   * @param taskConverter task converter
   */
  public FileBasedTaskQueue(QueueFile queueFile, TaskConverter<T> taskConverter) {
    this.queueFile = queueFile;
    this.taskConverter = taskConverter;
    log.fine("Enumerating queue");
    this.queueFile.iterator().forEachRemaining(task -> {
      Integer weight = taskConverter.getWeight(task);
      if (weight != null) {
        currentWeight.addAndGet(weight);
      }
    });
    log.fine("Enumerated: " + currentWeight.get() + " items in " +  queueFile.size() + " tasks");
  }

  @Override
  public T peek() {
    try {
      if (this.head != null) {
        return this.head;
      }
      byte[] task = queueFile.peek();
      if (task == null) return null;
      this.head = taskConverter.fromBytes(task);
      return this.head;
    } catch (IOException ex) {
      throw Utils.<Error>throwAny(ex);
    }
  }

  @Override
  public void add(@Nonnull T entry) throws IOException {
    bytes.reset();
    taskConverter.serializeToStream(entry, bytes);
    queueFile.add(bytes.getArray(), 0, bytes.size());
    currentWeight.addAndGet(entry.weight());
  }

  @Override
  public void clear() throws IOException {
    queueFile.clear();
    this.head = null;
    this.currentWeight.set(0);
  }

  @Override
  public void remove() throws IOException {
    if (this.head == null) {
      byte[] task = queueFile.peek();
      if (task == null) return;
      this.head = taskConverter.fromBytes(task);
    }
    queueFile.remove();
    if(this.head != null) {
      int weight = this.head.weight();
      currentWeight.getAndUpdate(x -> x > weight ? x - weight : 0);
      this.head = null;
    }
  }

  @Override
  public int size() {
    return queueFile.size();
  }

  @Override
  public void close() throws IOException {
    queueFile.close();
  }

  @Nullable
  @Override
  public Long weight() {
    return currentWeight.get();
  }

  @Nullable
  @Override
  public Long getAvailableBytes()  {
    return queueFile.storageBytes() - queueFile.usedBytes();
  }

  @Nonnull
  @Override
  public Iterator<T> iterator() {
    Iterator<byte[]> iterator = queueFile.iterator();
    return new Iterator<T>() {

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        byte[] data = iterator.next();
        try {
          return taskConverter.fromBytes(data);
        } catch (IOException e) {
          throw Utils.<Error>throwAny(e);
        }
      }

      @Override
      public void remove() {
        iterator.remove();
      }
    };
  }
}
