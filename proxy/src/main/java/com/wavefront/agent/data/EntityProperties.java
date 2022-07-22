package com.wavefront.agent.data;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import javax.annotation.Nullable;

/**
 * Unified interface for dynamic entity-specific dynamic properties, that may change at runtime
 *
 * @author vasily@wavefront.com
 */
public interface EntityProperties {
  // what we consider "unlimited"
  int NO_RATE_LIMIT = 10_000_000;
  int NO_RATE_LIMIT_BYTES = 1_000_000_000;

  // default values for dynamic properties
  boolean DEFAULT_SPLIT_PUSH_WHEN_RATE_LIMITED = false;
  double DEFAULT_RETRY_BACKOFF_BASE_SECONDS = 2.0d;
  int DEFAULT_FLUSH_INTERVAL = 1000;
  int DEFAULT_MAX_BURST_SECONDS = 10;
  int DEFAULT_BATCH_SIZE = 40000;
  int DEFAULT_BATCH_SIZE_HISTOGRAMS = 10000;
  int DEFAULT_BATCH_SIZE_SOURCE_TAGS = 50;
  int DEFAULT_BATCH_SIZE_SPANS = 5000;
  int DEFAULT_BATCH_SIZE_SPAN_LOGS = 1000;
  int DEFAULT_BATCH_SIZE_EVENTS = 50;
  int DEFAULT_MIN_SPLIT_BATCH_SIZE = 100;
  int DEFAULT_FLUSH_THREADS_SOURCE_TAGS = 2;
  int DEFAULT_FLUSH_THREADS_EVENTS = 2;

  // the maximum batch size for logs is set between 1 and 5 mb, with a default of 4mb
  int DEFAULT_MIN_SPLIT_BATCH_SIZE_LOGS_PAYLOAD = 1024 * 1024;
  int DEFAULT_BATCH_SIZE_LOGS_PAYLOAD = 4 * 1024 * 1024;
  int MAX_BATCH_SIZE_LOGS_PAYLOAD = 5 * 1024 * 1024;

  /**
   * Get initially configured batch size.
   *
   * @return batch size
   */
  int getDataPerBatchOriginal();

  /**
   * Get initially configured rate limit (per second).
   *
   * @return rate limit
   */
  double getRateLimit();

  /**
   * Get max number of burst seconds to allow when rate limiting to smooth out uneven traffic.
   *
   * @return number of seconds
   */
  int getRateLimitMaxBurstSeconds();

  /**
   * Get specific {@link RecyclableRateLimiter} instance.
   *
   * @return rate limiter
   */
  RecyclableRateLimiter getRateLimiter();

  /**
   * Get the number of worker threads.
   *
   * @return number of threads
   */
  int getFlushThreads();

  /**
   * Get interval between batches (in milliseconds)
   *
   * @return interval between batches
   */
  int getPushFlushInterval();

  /**
   * Get the maximum allowed number of items per single flush.
   *
   * @return batch size
   */
  int getDataPerBatch();

  /**
   * Sets the maximum allowed number of items per single flush.
   *
   * @param dataPerBatch batch size. if null is provided, reverts to originally configured value.
   */
  void setDataPerBatch(@Nullable Integer dataPerBatch);

  /**
   * Checks whether data flow for this entity type is disabled.
   *
   * @return true if data flow is disabled
   */
  boolean isFeatureDisabled();

  /**
   * Sets the flag value for "feature disabled" flag.
   *
   * @param featureDisabled if "true", data flow for this entity type is disabled.
   */
  void setFeatureDisabled(boolean featureDisabled);
}
