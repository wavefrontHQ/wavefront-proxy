package com.wavefront.agent.data;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.common.util.concurrent.RecyclableRateLimiterImpl;
import com.google.common.util.concurrent.RecyclableRateLimiterWithMetrics;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityRateLimiter {
  private Logger log = LoggerFactory.getLogger(this.getClass().getCanonicalName());

  private final RecyclableRateLimiterWithMetrics pointsLimit;
  private AtomicBoolean paused = new AtomicBoolean(false);

  public EntityRateLimiter() {
    this(Double.MAX_VALUE, Integer.MAX_VALUE, "unlimited");
  }

  public EntityRateLimiter(double rateLimit, int rateLimitMaxBurstSeconds, String prefix) {
    pointsLimit =
        new RecyclableRateLimiterWithMetrics(
            RecyclableRateLimiterImpl.create(rateLimit, rateLimitMaxBurstSeconds), prefix);
  }

  public void pause() {
    if (!paused.get()) {
      paused.set(true);
      try {
        Thread.sleep(MINUTES.toMillis(1));
        paused.set(false);
      } catch (InterruptedException e) {
        log.error("error", e);
        paused.set(false);
      }
    }
  }

  public void setRate(double rate) {
    pointsLimit.setRate(rate);
  }

  public double getRate() {
    return pointsLimit.getRate();
  }

  public boolean tryAcquire(int points) {
    if (!paused.get()) {
      return pointsLimit.tryAcquire(points);
    }
    return false;
  }
}
