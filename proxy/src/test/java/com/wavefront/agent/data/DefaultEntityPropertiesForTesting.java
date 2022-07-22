package com.wavefront.agent.data;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiterImpl;
import javax.annotation.Nullable;

/** @author vasily@wavefront.com */
public class DefaultEntityPropertiesForTesting implements EntityProperties {

  @Override
  public int getDataPerBatchOriginal() {
    return DEFAULT_BATCH_SIZE;
  }

  @Override
  public double getRateLimit() {
    return NO_RATE_LIMIT;
  }

  @Override
  public int getRateLimitMaxBurstSeconds() {
    return DEFAULT_MAX_BURST_SECONDS;
  }

  @Override
  public RecyclableRateLimiter getRateLimiter() {
    return RecyclableRateLimiterImpl.create(NO_RATE_LIMIT, getRateLimitMaxBurstSeconds());
  }

  @Override
  public int getFlushThreads() {
    return 2;
  }

  @Override
  public int getPushFlushInterval() {
    return 100000;
  }

  @Override
  public int getDataPerBatch() {
    return DEFAULT_BATCH_SIZE;
  }

  @Override
  public void setDataPerBatch(@Nullable Integer dataPerBatch) {}

  @Override
  public boolean isFeatureDisabled() {
    return false;
  }

  @Override
  public void setFeatureDisabled(boolean featureDisabled) {}
}
