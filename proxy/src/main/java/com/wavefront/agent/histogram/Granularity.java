package com.wavefront.agent.histogram;

import org.apache.commons.lang.time.DateUtils;

import javax.annotation.Nullable;

/**
 * Standard supported aggregation Granularities.
 * Refactored from HistogramUtils.
 *
 * @author Tim Schmidt (tim@wavefront.com)
 * @author vasily@wavefront.com
 */
public enum Granularity {
  MINUTE((int) DateUtils.MILLIS_PER_MINUTE),
  HOUR((int) DateUtils.MILLIS_PER_HOUR),
  DAY((int) DateUtils.MILLIS_PER_DAY);

  private final int inMillis;

  Granularity(int inMillis) {
    this.inMillis = inMillis;
  }

  /**
   * Duration of a corresponding bin in milliseconds.
   *
   * @return bin length in milliseconds
   */
  public int getInMillis() {
    return inMillis;
  }

  /**
   * Bin id for an epoch time is the epoch time in the corresponding granularity.
   *
   * @param timeMillis epoch time in milliseconds
   * @return the bin id
   */
  public int getBinId(long timeMillis) {
    return (int) (timeMillis / inMillis);
  }

  @Override
  public String toString() {
    switch (this) {
      case DAY:
        return "day";
      case HOUR:
        return "hour";
      case MINUTE:
        return "minute";
    }
    return "unknown";
  }

  public static Granularity fromMillis(long millis) {
    if (millis <= 60 * 1000) {
      return MINUTE;
    } else if (millis <= 60 * 60 * 1000) {
      return HOUR;
    } else {
      return DAY;
    }
  }

}
