package com.wavefront.common;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

public class MinuteBin {
  private final TDigest dist;
  private final long minMillis;

  public MinuteBin(int accuracy, long minMillis) {
    dist = new AVLTreeDigest(accuracy);
    this.minMillis = minMillis;
  }

  public TDigest getDist() {
    return dist;
  }

  public long getMinMillis() {
    return minMillis;
  }
}
