package com.wavefront.agent.histogram.accumulator;

import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.histogram.HistogramKey;
import com.wavefront.common.TimeProvider;

import java.util.Iterator;
import java.util.function.BiFunction;

import javax.annotation.Nonnull;

import wavefront.report.Histogram;

/**
 * Caching wrapper around the backing store.
 *
 * @author vasily@wavefront.com
 */
public interface Accumulator {

  /**
   * Update {@code AgentDigest} in the cache with another {@code AgentDigest}.
   *
   * @param key   histogram key
   * @param value {@code AgentDigest} to be merged
   */
  void put(HistogramKey key, @Nonnull AgentDigest value);

  /**
   * Update {@link AgentDigest} in the cache with a double value. If such {@code AgentDigest} does
   * not exist for the specified key, it will be created using {@link AgentDigestFactory}
   *
   * @param key histogram key
   * @param value value to be merged into the {@code AgentDigest}
   */
  void put(HistogramKey key, double value);

  /**
   * Update {@link AgentDigest} in the cache with a {@code Histogram} value. If such
   * {@code AgentDigest} does not exist for the specified key, it will be created
   * using {@link AgentDigestFactory}.
   *
   * @param key histogram key
   * @param value a {@code Histogram} to be merged into the {@code AgentDigest}
   */
  void put(HistogramKey key, Histogram value);

  /**
   * Attempts to compute a mapping for the specified key and its current mapped value
   * (or null if there is no current mapping).
   *
   * @param key               key with which the specified value is to be associated
   * @param remappingFunction the function to compute a value
   * @return                  the new value associated with the specified key, or null if none
   */
  AgentDigest compute(HistogramKey key, BiFunction<? super HistogramKey,
      ? super AgentDigest, ? extends AgentDigest> remappingFunction);

  /**
   * Returns an iterator over "ripe" digests ready to be shipped
   *
   * @param clock a millisecond-precision epoch time source
   * @return an iterator over "ripe" digests ready to be shipped
   */
  Iterator<HistogramKey> getRipeDigestsIterator(TimeProvider clock);

  /**
   * Returns the number of items in the storage behind the cache
   *
   * @return number of items
   */
  long size();

  /**
   * Merge the contents of this cache with the corresponding backing store.
   */
  void flush();
}
