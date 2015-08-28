package com.wavefront.api.agent;

/**
 * What stage of development is this metric in? This is intended for public consumption.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public enum MetricStage {
  TRIAL,          // Should only be run once on a target, unless it's changed
  PER_FETCH,      // Should be run once each time the daemon phones home
  ACTIVE          // Should be run in a continuous loop, based on delay
}
