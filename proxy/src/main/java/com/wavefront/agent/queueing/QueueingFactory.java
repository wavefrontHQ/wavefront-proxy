package com.wavefront.agent.queueing;

import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;

import javax.annotation.Nonnull;

/**
 * Factory for {@link QueueProcessor} instances.
 *
 * @author vasily@wavefront.com
 */
public interface QueueingFactory {
  /**
   * Create a new {@code QueueController} instance for the specified handler key.
   *
   * @param handlerKey   {@link HandlerKey} for the queue controller.
   * @param numThreads   number of threads to create processor tasks for.
   * @param <T>          data submission task type.
   * @return {@code QueueController} object
   */
  <T extends DataSubmissionTask<T>> QueueController<T> getQueueController(
      @Nonnull HandlerKey handlerKey, int numThreads);
}
