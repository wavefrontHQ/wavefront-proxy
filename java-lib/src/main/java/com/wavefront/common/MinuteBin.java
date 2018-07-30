package com.wavefront.common;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

/**
 * Representation of a bin that holds histogram data for a particular minute in time.
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public class MinuteBin {
  private final TDigest dist;

  /**
   * timestamp at the start of the minute
   */
  private final long minuteMillis;

  public MinuteBin(int accuracy, long minuteMillis) {
    dist = new AVLTreeDigest(accuracy);
    this.minuteMillis = minuteMillis;
  }

  public TDigest getDist() {
    return dist;
  }

  public long getMinuteMillis() { return minuteMillis; }
}
