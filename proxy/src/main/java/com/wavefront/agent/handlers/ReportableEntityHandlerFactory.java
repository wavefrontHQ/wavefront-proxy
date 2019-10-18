package com.wavefront.agent.handlers;

import com.wavefront.data.ReportableEntityType;

/**
 * Factory for {@link ReportableEntityHandler} objects.
 *
 * @author vasily@wavefront.com
 */
public interface ReportableEntityHandlerFactory {

  /**
   * Create, or return existing, {@link ReportableEntityHandler}.
   *
   * @param handlerKey unique identifier for the handler.
   * @return new or existing handler.
   */
  ReportableEntityHandler getHandler(HandlerKey handlerKey);

  /**
   * Create, or return existing, {@link ReportableEntityHandler}.
   *
   * @param  entityType ReportableEntityType for the handler.
   * @param  handle     handle.
   * @return new or existing handler.
   */
  default ReportableEntityHandler getHandler(ReportableEntityType entityType, String handle) {
    return getHandler(HandlerKey.of(entityType, handle));
  }

  /**
   * Perform finalizing tasks on handlers.
   */
  default void shutdown() {
    // no-op
  }

}
