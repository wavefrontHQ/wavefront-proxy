package com.wavefront.agent.queueing;

import com.wavefront.agent.data.DataSubmissionTask;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Proxy-specific queue interface, which is basically a wrapper for a Tape queue. This allows us to
 * potentially support more than one backing storage in the future.
 *
 * @param <T> type of objects stored.
 *
 * @author vasily@wavefront.com.
 */
public interface TaskQueue<T extends DataSubmissionTask<T>> {

  /**
   * Retrieve a task that is currently at the head of the queue.
   *
   * @return task object
   */
  T peek();// throws IOException;

  /**
   * Add a task to the end of the queue
   *
   * @param t task
   * @throws IOException IO exceptions caught by the storage engine
   */
  void add(@Nonnull T t) throws IOException;

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
  void clear();

  /**
   * Returns a number of tasks currently in the queue.
   *
   * @return number of tasks
   */
  int size();

  /**
   * Close the queue. Should be invoked before a graceful shutdown.
   */
  void close();

  /**
   * Returns the total weight of the queue (sum of weights of all tasks).
   *
   * @return weight of the queue (null if unknown)
   */
  Long weight();
}
