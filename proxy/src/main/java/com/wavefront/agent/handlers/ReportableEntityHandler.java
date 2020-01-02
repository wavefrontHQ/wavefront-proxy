package com.wavefront.agent.handlers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Handler that processes incoming objects of a single entity type, validates them and
 * hands them over to one of the {@link SenderTask} threads.
 *
 * @author vasily@wavefront.com
 *
 * @param <T> the type of input objects handled.
 */
public interface ReportableEntityHandler<T, U> {

  /**
   * Validate and accept the input object.
   *
   * @param t object to accept.
   */
  void report(T t);

  /**
   * Handle the input object as blocked. Blocked objects are otherwise valid objects
   * that are rejected based on user-defined criteria.
   *
   * @param t object to block.
   */
  void block(T t);

  /**
   * Handle the input object as blocked. Blocked objects are otherwise valid objects
   * that are rejected based on user-defined criteria.
   *
   * @param t       object to block.
   * @param message message to write to the main log.
   */
  void block(@Nullable T t, @Nullable String message);

  /**
   * Reject the input object as invalid, i.e. rejected based on criteria defined by Wavefront.
   *
   * @param t object to reject.
   * @param message more user-friendly message to write to the main log.
   */
  void reject(@Nullable T t, @Nullable String message);

  /**
   * Reject the input object as invalid, i.e. rejected based on criteria defined by Wavefront.
   *
   * @param t string to reject and to write to RawBlockedPointsLog.
   * @param message more user-friendly message to write to the main log.
   */
  void reject(@Nonnull String t, @Nullable String message);

  /**
   * Gracefully shutdown the pipeline.
   */
  void shutdown();
}
