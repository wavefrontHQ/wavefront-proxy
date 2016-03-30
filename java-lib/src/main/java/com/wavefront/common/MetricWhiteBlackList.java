/**
 */
package com.wavefront.common;

import javax.annotation.Nullable;
import java.util.regex.Pattern;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import org.apache.commons.lang.StringUtils;

public class MetricWhiteBlackList
{
  @Nullable
  private final Pattern pointLineWhiteList;
  @Nullable
  private final Pattern pointLineBlackList;

  private final Counter regexRejects;

  public MetricWhiteBlackList(@Nullable final String pointLineWhiteListRegex,
                              @Nullable final String pointLineBlackListRegex,
                              final String rejectCounterName) {

    this.pointLineWhiteList = StringUtils.isBlank(pointLineWhiteListRegex) ?
        null : Pattern.compile(pointLineWhiteListRegex);

    this.pointLineBlackList = StringUtils.isBlank(pointLineBlackListRegex) ?
        null : Pattern.compile(pointLineBlackListRegex);

    this.regexRejects = Metrics.newCounter(
      new MetricName("validationRegex." + rejectCounterName, "", "points-rejected"));

  }

  public boolean passes(String pointLine) {
    if ((pointLineWhiteList != null &&
         !pointLineWhiteList.matcher(pointLine).matches()) ||
        (pointLineBlackList != null &&
         pointLineBlackList.matcher(pointLine).matches()))
    {
      regexRejects.inc();
      return false;
    }
    return true;
  }

}
