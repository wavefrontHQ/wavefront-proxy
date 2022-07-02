package com.wavefront.agent.core.handlers;

import javax.annotation.Nonnull;

/** Factory for {@link ReportableEntityHandler} objects. */
public interface ReportableEntityHandlerFactory {

  /**
   * Create, or return existing, {@link ReportableEntityHandler}.
   *
   * @param handler
   * @param queue unique identifier for the handler.
   * @return new or existing handler.
   */
  <T, U> ReportableEntityHandler<T, U> getHandler(
      String handler, com.wavefront.agent.core.queues.QueueInfo queue);

  default <T, U> ReportableEntityHandler<T, U> getHandler(
      int port, com.wavefront.agent.core.queues.QueueInfo queue) {
    return getHandler(String.valueOf(port), queue);
  }

  /** Shutdown pipeline for a specific handle. */
  void shutdown(@Nonnull int handle);
}
