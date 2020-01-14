package com.wavefront.agent.handlers;

import com.wavefront.data.ReportableEntityType;

import javax.annotation.Nonnull;

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
  <T, U> ReportableEntityHandler<T, U> getHandler(HandlerKey handlerKey);

  /**
   * Create, or return existing, {@link ReportableEntityHandler}.
   *
   * @param  entityType ReportableEntityType for the handler.
   * @param  handle     handle.
   * @return new or existing handler.
   */
  default <T, U> ReportableEntityHandler<T, U> getHandler(
      ReportableEntityType entityType, String handle) {
    return getHandler(HandlerKey.of(entityType, handle));
  }

  /**
   * Shutdown pipeline for a specific handle.
   */
  void shutdown(@Nonnull String handle);
}
