package com.wavefront.agent.queueing;

import com.wavefront.agent.data.DataSubmissionTask;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Proxy-specific queue interface, which is basically a wrapper for a Tape queue. This allows us to
 * potentially support more than one backing storage in the future.
 *
 * @param <T> type of objects stored.
 *
 * @author vasily@wavefront.com.
 */
public interface TaskQueue<T extends DataSubmissionTask<T>> extends Iterable<T> {

  /**
   * Retrieve a task that is currently at the head of the queue.
   *
   * @return task object
   */
  @Nullable
  T peek();

  /**
   * Add a task to the end of the queue
   *
   * @param entry task
   * @throws IOException IO exceptions caught by the storage engine
   */
  void add(@Nonnull T entry) throws IOException;

  /**
   * Remove a task from the head of the queue. Requires peek() to be called first, otherwise
   * an {@code IllegalStateException} is thrown.
   *
   * @throws IOException IO exceptions caught by the storage engine
   */
  void remove() throws IOException;

  /**
   * Empty and re-initialize the queue.
   */
  void clear() throws IOException;

  /**
   * Returns a number of tasks currently in the queue.
   *
   * @return number of tasks
   */
  int size();

  /**
   * Close the queue. Should be invoked before a graceful shutdown.
   */
  void close() throws IOException;

  /**
   * Returns the total weight of the queue (sum of weights of all tasks).
   *
   * @return weight of the queue (null if unknown)
   */
  @Nullable
  Long weight();

  /**
   * Returns the total number of pre-allocated but unused bytes in the backing file.
   * May return null if not applicable.
   *
   * @return total number of available bytes in the file or null
   */
  @Nullable
  Long getAvailableBytes();

  /**
   * Returns the unique queue name/identifier, can be anything, like file path name for
   * file-based queues.
   *
   * @return queue name.
   */
  String getName();
}
