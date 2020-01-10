package com.wavefront.common;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.util.concurrent.RateLimiter;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A logger that suppresses identical messages for a specified period of time.
 *
 * @author vasily@wavefront.com
 */
@SuppressWarnings("UnstableApiUsage")
public class MessageDedupingLogger extends DelegatingLogger {
  private final LoadingCache<String, RateLimiter> rateLimiterCache;

  /**
   * @param delegate     Delegate logger.
   * @param maximumSize  max number of unique messages that can exist in the cache
   * @param rateLimit    rate limit (per second per each unique message)
   */
  public MessageDedupingLogger(Logger delegate,
                               long maximumSize,
                               double rateLimit) {
    super(delegate);
    this.rateLimiterCache = Caffeine.newBuilder().
        expireAfterAccess((long)(2 / rateLimit), TimeUnit.SECONDS).
        maximumSize(maximumSize).
        build(x -> RateLimiter.create(rateLimit));
  }

  @Override
  public void log(Level level, String message) {
    if (Objects.requireNonNull(rateLimiterCache.get(message)).tryAcquire()) {
      log(new LogRecord(level, message));
    }
  }
}
