package com.wavefront.agent.handlers;

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
   * Perform finalizing tasks on handlers.
   */
  default void shutdown() {
    // no-op
  }

}
