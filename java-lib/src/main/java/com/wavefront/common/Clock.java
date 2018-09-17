package com.wavefront.common;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

/**
 * Clock to manage agent time synced with the server.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public abstract class Clock {
  private static Long serverTime;
  private static Long localTime;
  private static Long clockDrift;

  static {
    Metrics.newGauge(new MetricName("clock", "", "drift"), new Gauge<Long>() {
      @Override
      public Long value() {
        return clockDrift == null ? null : (long)Math.floor(clockDrift / 1000 + 0.5d);
      }
    });
  }

  public static void set(long serverTime) {
    localTime = System.currentTimeMillis();
    Clock.serverTime = serverTime;
    clockDrift = serverTime - localTime;
  }

  public static long now() {
    if (serverTime == null) return System.currentTimeMillis();
    else return (System.currentTimeMillis() - localTime) + serverTime;
  }
}
