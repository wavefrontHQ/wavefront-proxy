package com.wavefront.data;

/**
 * Type of objects that Wavefront proxy can send to the server endpoint(s).
 *
 * @author vasily@wavefront.com
 */
public enum ReportableEntityType {
  POINT("points"),
  HISTOGRAM("points"),
  SOURCE_TAG("sourceTags"),
  TRACE("traces");

  private final String name;

  ReportableEntityType(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }
}
