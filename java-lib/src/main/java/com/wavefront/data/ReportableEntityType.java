package com.wavefront.data;

/**
 * Type of objects that Wavefront proxy can send to the server endpoint(s).
 *
 * @author vasily@wavefront.com
 */
public enum ReportableEntityType {
  POINT("points"),
  DELTA_COUNTER("deltaCounter"),
  HISTOGRAM("histograms"),
  SOURCE_TAG("sourceTags"),
  TRACE("spans"),
  TRACE_SPAN_LOGS("spanLogs");

  private final String name;

  ReportableEntityType(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }

  public String toCapitalizedString() {
    return name.substring(0, 1).toUpperCase() + name.substring(1);
  }
}
