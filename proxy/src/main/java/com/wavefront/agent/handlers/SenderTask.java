package com.wavefront.agent.handlers;

import com.wavefront.agent.data.QueueingReason;

import java.util.concurrent.ExecutorService;

/**
 * Batch and ship valid items to Wavefront servers
 *
 * @author vasily@wavefront.com
 *
 * @param <T> the type of input objects handled.
 */
public interface SenderTask<T> {

  /**
   * Add valid item to the send queue (memory buffers).
   *
   * @param item item to add to the send queue.
   */
  void add(T item);

  /**
   * Add multiple valid items to the send queue (memory buffers).
   *
   * @param items items to add to the send queue.
   */
  default void add(Iterable<T> items) {
    for (T item : items) {
      add(item);
    }
  }

  /**
   * Calculate a numeric score (the lower the better) that is intended to help the {@link ReportableEntityHandler}
   * to choose the best SenderTask to handle over data to.
   *
   * @return task score
   */
  long getTaskRelativeScore();

  /**
   * Force memory buffer flush.
   */
  void drainBuffersToQueue(QueueingReason reason);

  /**
   * Shut down the scheduler for this task (prevent future scheduled runs).
   *
   * @return executor service that is shutting down.
   */
  ExecutorService shutdown();
}
