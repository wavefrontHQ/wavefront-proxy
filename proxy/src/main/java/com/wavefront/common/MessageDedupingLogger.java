package com.wavefront.common;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.util.concurrent.RateLimiter;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A logger that suppresses identical messages for a specified period of time.
 *
 * @author vasily@wavefront.com
 */
@SuppressWarnings("UnstableApiUsage")
public class MessageDedupingLogger extends Logger {
  private final Logger delegate;
  private final LoadingCache<String, RateLimiter> rateLimiterCache;

  /**
   * @param loggerName   logger name.
   * @param maximumSize  max number of unique messages that can exist in the cache
   * @param rateLimit    rate limit per message
   */
  public MessageDedupingLogger(String loggerName,
                               long maximumSize,
                               double rateLimit) {
    super(loggerName, null);
    this.delegate = Logger.getLogger(loggerName);
    this.rateLimiterCache = Caffeine.newBuilder().
        expireAfterAccess((long)(2 / rateLimit), TimeUnit.SECONDS).
        maximumSize(maximumSize).
        build(x -> RateLimiter.create(rateLimit));
  }

  @Override
  public void severe(String msg) {
    log(Level.SEVERE, msg);
  }

  @Override
  public void warning(String msg) {
    log(Level.WARNING, msg);
  }

  @Override
  public void info(String msg) {
    log(Level.INFO, msg);
  }

  @Override
  public void config(String msg) {
    log(Level.CONFIG, msg);
  }

  @Override
  public void fine(String msg) {
    log(Level.FINE, msg);
  }

  @Override
  public void finer(String msg) {
    log(Level.FINER, msg);
  }

  @Override
  public void finest(String msg) {
    log(Level.FINEST, msg);
  }

  /**
   * @param level   log level.
   * @param message string to write to log.
   */
  @Override
  public void log(Level level, String message) {
    if (Objects.requireNonNull(rateLimiterCache.get(message)).tryAcquire()) {
      delegate.log(level, message);
    }
  }
}
