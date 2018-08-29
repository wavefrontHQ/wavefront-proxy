package com.wavefront.agent.handlers;

import java.util.Collection;

import javax.validation.constraints.NotNull;

/**
 * Factory for {@link SenderTask} objects.
 *
 * @author vasily@wavefront.com
 */
public interface SenderTaskFactory {

  /**
   * Create a collection of {@link SenderTask objects} for a specified handler key.
   *
   * @param handlerKey unique identifier for the handler.
   * @param numThreads create a specified number of threads.
   * @return created tasks.
   */
  Collection<SenderTask> createSenderTasks(@NotNull HandlerKey handlerKey,
                                           final int numThreads);

  /**
   * Shut down all tasks.
   */
  void shutdown();
}
