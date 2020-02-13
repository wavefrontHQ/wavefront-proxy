package com.wavefront.common;

import com.google.common.util.concurrent.RateLimiter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A rate-limiting logger that can be shared between multiple threads
 * that use the same context key.
 */
@SuppressWarnings("UnstableApiUsage")
public class SharedRateLimitingLogger extends DelegatingLogger {
  private static final Map<String, RateLimiter> SHARED_CACHE = new ConcurrentHashMap<>();

  private final RateLimiter rateLimiter;

  /**
   * @param delegate     Delegate logger.
   * @param context      Shared context key.
   * @param rateLimit    Rate limit (messages per second)
   */
  public SharedRateLimitingLogger(Logger delegate, String context, double rateLimit) {
    super(delegate);
    this.rateLimiter = SHARED_CACHE.computeIfAbsent(context, x -> RateLimiter.create(rateLimit));
  }

  /**
   * @param level   log level.
   * @param message string to write to log.
   */
  @Override
  public void log(Level level, String message) {
    if (!delegate.isLoggable(level)) {
      return;
    }
    if (rateLimiter.tryAcquire()) {
      log(new LogRecord(level, message));
    }
  }

  /**
   * @param level           Log level.
   * @param messageSupplier A function, which when called, produces the desired log message.
   */
  @Override
  public void log(Level level, Supplier<String> messageSupplier) {
    if (!delegate.isLoggable(level)) {
      return;
    }
    if (rateLimiter.tryAcquire()) {
      log(new LogRecord(level, messageSupplier.get()));
    }
  }
}
