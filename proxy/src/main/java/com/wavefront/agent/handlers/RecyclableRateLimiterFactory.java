package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RecyclableRateLimiter;

import javax.annotation.Nullable;

/**
 * Factory for {@link RecyclableRateLimiter} instances.
 *
 * @author vasily@wavefront.com
 */
public interface RecyclableRateLimiterFactory {

  /**
   * Get a {@link RecyclableRateLimiter} instance for specified handler key.
   *
   * @param handlerKey handler key to return a {@link RecyclableRateLimiter} instance for.
   * @return rate limiter
   */
  @Nullable
  RecyclableRateLimiter getRateLimiter(HandlerKey handlerKey);
}
