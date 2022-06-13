package com.wavefront.agent.handlers;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nonnull;

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
   * @return created tasks corresponding to different Wavefront endpoints {@link
   *     com.wavefront.api.ProxyV2API}.
   */
  <T> Map<String, Collection<SenderTask>> createSenderTasks(@Nonnull HandlerKey handlerKey);

  /** Shut down all tasks. */
  void shutdown();

  /**
   * Shut down specific pipeline
   *
   * @param handle pipeline's handle
   */
  void shutdown(@Nonnull String handle);

  void truncateBuffers();
}
