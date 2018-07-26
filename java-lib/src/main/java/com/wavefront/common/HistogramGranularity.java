package com.wavefront.common;

/**
 * Granularity (minute, hour, or day) by which histogram distributions are aggregated.
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public enum HistogramGranularity {
  MINUTE("!M"),
  HOUR("!H"),
  DAY("!D");

  public final String identifier;

  HistogramGranularity(String identifier) {
    this.identifier = identifier;
  }
}
