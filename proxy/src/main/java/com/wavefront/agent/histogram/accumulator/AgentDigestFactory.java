package com.wavefront.agent.histogram.accumulator;

import com.tdunning.math.stats.AgentDigest;
import com.wavefront.common.TimeProvider;

import java.util.function.Supplier;

/**
 * A simple factory for creating {@link AgentDigest} objects with a specific compression level
 * and expiration TTL.
 *
 * @author vasily@wavefront.com
 */
public class AgentDigestFactory {
  private final Supplier<Short> compressionSupplier;
  private final long ttlMillis;
  private final TimeProvider timeProvider;

  /**
   * @param compressionSupplier supplier for compression level setting.
   * @param ttlMillis           default ttlMillis for new digests.
   * @param timeProvider        time provider (in millis).
   */
  public AgentDigestFactory(Supplier<Short> compressionSupplier, long ttlMillis,
                            TimeProvider timeProvider) {
    this.compressionSupplier = compressionSupplier;
    this.ttlMillis = ttlMillis;
    this.timeProvider = timeProvider;
  }

  public AgentDigest newDigest() {
    return new AgentDigest(compressionSupplier.get(), timeProvider.currentTimeMillis() + ttlMillis);
  }
}
