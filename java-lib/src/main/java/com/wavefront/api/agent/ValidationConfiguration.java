package com.wavefront.api.agent;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Data validation settings. Retrieved by the proxy from the back-end during check-in process.
 *
 * @author vasily@wavefront.com
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ValidationConfiguration {

  /**
   * Maximum allowed metric name length. Default: 256 characters.
   */
  private int metricLengthLimit = 256;

  /**
   * Maximum allowed histogram metric name length. Default: 128 characters.
   */
  private int histogramLengthLimit = 128;

  /**
   * Maximum allowed span name length. Default: 128 characters.
   */
  private int spanLengthLimit = 128;

  /**
   * Maximum allowed host/source name length. Default: 128 characters.
   */
  private int hostLengthLimit = 128;

  /**
   * Maximum allowed number of point tags per point/histogram. Default: 20.
   */
  private int annotationsCountLimit = 20;

  /**
   * Maximum allowed length for point tag keys. Enforced in addition to 255 characters key + "=" + value limit.
   * Default: 64 characters.
   */
  private int annotationsKeyLengthLimit = 64;

  /**
   * Maximum allowed length for point tag values. Enforced in addition to 255 characters key + "=" + value limit.
   * Default: 255 characters.
   */
  private int annotationsValueLengthLimit = 255;

  /**
   * Maximum allowed number of annotations per span. Default: 20.
   */
  private int spanAnnotationsCountLimit = 20;

  /**
   * Maximum allowed length for span annotation keys. Enforced in addition to 255 characters key + "=" + value limit.
   * Default: 128 characters.
   */
  private int spanAnnotationsKeyLengthLimit = 128;

  /**
   * Maximum allowed length for span annotation values. Enforced in addition to 255 characters key + "=" + value limit.
   * Default: 128 characters.
   */
  private int spanAnnotationsValueLengthLimit = 128;

  public int getMetricLengthLimit() {
    return metricLengthLimit;
  }

  public int getHistogramLengthLimit() {
    return histogramLengthLimit;
  }

  public int getSpanLengthLimit() {
    return spanLengthLimit;
  }

  public int getHostLengthLimit() {
    return hostLengthLimit;
  }

  public int getAnnotationsCountLimit() {
    return annotationsCountLimit;
  }

  public int getAnnotationsKeyLengthLimit() {
    return annotationsKeyLengthLimit;
  }

  public int getAnnotationsValueLengthLimit() {
    return annotationsValueLengthLimit;
  }

  public int getSpanAnnotationsCountLimit() {
    return spanAnnotationsCountLimit;
  }

  public int getSpanAnnotationsKeyLengthLimit() {
    return spanAnnotationsKeyLengthLimit;
  }

  public int getSpanAnnotationsValueLengthLimit() {
    return spanAnnotationsValueLengthLimit;
  }

  public ValidationConfiguration setMetricLengthLimit(int value) {
    this.metricLengthLimit = value;
    return this;
  }

  public ValidationConfiguration setHistogramLengthLimit(int value) {
    this.histogramLengthLimit = value;
    return this;
  }

  public ValidationConfiguration setSpanLengthLimit(int value) {
    this.spanLengthLimit = value;
    return this;
  }

  public ValidationConfiguration setHostLengthLimit(int value) {
    this.hostLengthLimit = value;
    return this;
  }

  public ValidationConfiguration setAnnotationsKeyLengthLimit(int value) {
    this.annotationsKeyLengthLimit = value;
    return this;
  }

  public ValidationConfiguration setAnnotationsValueLengthLimit(int value) {
    this.annotationsValueLengthLimit = value;
    return this;
  }

  public ValidationConfiguration setAnnotationsCountLimit(int value) {
    this.annotationsCountLimit = value;
    return this;
  }

  public ValidationConfiguration setSpanAnnotationsKeyLengthLimit(int value) {
    this.spanAnnotationsKeyLengthLimit = value;
    return this;
  }

  public ValidationConfiguration setSpanAnnotationsValueLengthLimit(int value) {
    this.spanAnnotationsValueLengthLimit = value;
    return this;
  }

  public ValidationConfiguration setSpanAnnotationsCountLimit(int value) {
    this.spanAnnotationsCountLimit = value;
    return this;
  }
}
