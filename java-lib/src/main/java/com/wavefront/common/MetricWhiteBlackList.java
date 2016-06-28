package com.wavefront.common;

import com.google.common.base.Predicate;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * White/Black list checker for a metric.  This code was originally contained within the ChannelStringHandler.  This
 * class was created for easy re-use by classes such as the ChannelByteArrayHandler.
 *
 * @author Mike McLaughlin (mike@wavefront.com)
 */
public class MetricWhiteBlackList implements Predicate<String> {
  @Nullable
  private final Pattern pointLineWhiteList;
  @Nullable
  private final Pattern pointLineBlackList;

  /**
   * Counter for number of rejected metrics.
   */
  private final Counter regexRejects;

  /**
   * Constructor.
   *
   * @param pointLineWhiteListRegex the white list regular expression.
   * @param pointLineBlackListRegex the black list regular expression
   * @param portName                the port used as metric name (validationRegex.point-rejected [port=portName])
   */
  public MetricWhiteBlackList(@Nullable final String pointLineWhiteListRegex,
                              @Nullable final String pointLineBlackListRegex,
                              final String portName) {

    if (!StringUtils.isBlank(pointLineWhiteListRegex)) {
      this.pointLineWhiteList = Pattern.compile(pointLineWhiteListRegex);
    } else {
      this.pointLineWhiteList = null;
    }
    if (!StringUtils.isBlank(pointLineBlackListRegex)) {
      this.pointLineBlackList = Pattern.compile(pointLineBlackListRegex);
    } else {
      this.pointLineBlackList = null;
    }

    this.regexRejects = Metrics.newCounter(
        new TaggedMetricName("validationRegex", "points-rejected", "port", portName));
  }

  /**
   * Check to see if the given point line or metric passes the white and black list.
   *
   * @param pointLine the line to check
   * @return true if the line passes checks; false o/w
   */
  @Override
  public boolean apply(String pointLine) {
    if ((pointLineWhiteList != null && !pointLineWhiteList.matcher(pointLine).matches()) ||
        (pointLineBlackList != null && pointLineBlackList.matcher(pointLine).matches())) {
      regexRejects.inc();
      return false;
    }
    return true;
  }
}
