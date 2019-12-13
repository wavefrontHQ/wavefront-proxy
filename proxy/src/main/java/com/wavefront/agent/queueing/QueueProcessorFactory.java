package com.wavefront.agent.queueing;

import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;

import javax.validation.constraints.NotNull;

/**
 * Factory for {@link QueueProcessor} instances.
 *
 * @author vasily@wavefront.com
 */
public interface QueueProcessorFactory {
  /**
   * Create a new {@code QueueProcessor} instance for a specified handler key.

   * @param handlerKey   {@link HandlerKey} for the queue processor.
   * @param threadNum    thread number
   * @param <T>          data submission task type
   *
   * @return {@code QueueProcessor} object
   */
  <T extends DataSubmissionTask<T>> QueueProcessor<T> getQueueProcessor(
      @NotNull HandlerKey handlerKey, int threadNum);
}
