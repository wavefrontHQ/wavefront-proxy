package com.wavefront.agent.sampler;

/**
 * Sampler that samples incoming objects of a particular type.
 *
 * @author Vikram Raman (vikram@wavefront.com)
 *
 * @param <T> The type of input objects handled.
 */
public interface ReportableEntitySampler<T> {

  /**
   * Samples the given object.
   *
   * @param t The object to sample
   * @return true if the object should be reported to Wavefront, false otherwise
   */
  boolean sample(T t);
}
