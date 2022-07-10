package com.wavefront.agent.core.handlers;

import com.wavefront.agent.core.queues.QueueInfo;
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
  <T> ReportableEntityHandler<T> getHandler(String handler, QueueInfo queue);

  default <T> ReportableEntityHandler<T> getHandler(int port, QueueInfo queue) {
    return getHandler(String.valueOf(port), queue);
  }

  /** Shutdown pipeline for a specific handle. */
  void shutdown(@Nonnull int handle);
}
