package com.wavefront.agent.handlers;

import com.wavefront.agent.data.QueueingReason;
import com.wavefront.common.Managed;

import javax.annotation.Nullable;

/**
 * Batch and ship valid items to Wavefront servers
 *
 * @author vasily@wavefront.com
 *
 * @param <T> the type of input objects handled.
 */
public interface SenderTask<T> extends Managed {

  /**
   * Add valid item to the send queue (memory buffers).
   *
   * @param item item to add to the send queue.
   */
  void add(T item);

  /**
   * Calculate a numeric score (the lower the better) that is intended to help the
   * {@link ReportableEntityHandler} choose the best SenderTask to handle over data to.
   *
   * @return task score
   */
  long getTaskRelativeScore();

  /**
   * Force memory buffer flush.
   *
   * @param reason reason for queueing.
   */
  void drainBuffersToQueue(@Nullable QueueingReason reason);
}
