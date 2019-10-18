package com.wavefront.agent.histogram.accumulator;

import com.google.common.annotations.VisibleForTesting;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.TimeProvider;

/**
 * A simple factory for creating {@link AgentDigest} objects with a specific compression level
 * and expiration TTL.
 *
 * @author vasily@wavefront.com
 */
public class AgentDigestFactory {
  private final short compression;
  private final long ttlMillis;
  private final TimeProvider timeProvider;

  public AgentDigestFactory(short compression, long ttlMillis) {
    this(compression, ttlMillis, System::currentTimeMillis);
  }

  @VisibleForTesting
  AgentDigestFactory(short compression, long ttlMillis, TimeProvider timeProvider) {
    this.compression = compression;
    this.ttlMillis = ttlMillis;
    this.timeProvider = timeProvider;
  }

  public AgentDigest newDigest() {
    return new AgentDigest(compression, timeProvider.millisSinceEpoch() + ttlMillis);
  }
}
