package com.wavefront.agent.preprocessor;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.time.DateUtils;

import sunnylabs.report.ReportPoint;

/**
 * Filter condition for valid timestamp - should be no more than 1 day in the future
 * and no more than X hours (usually 8760, or 1 year) in the past
 *
 * Created by Vasily on 9/16/16.
 */
public class ReportPointTimestampInRangeFilter extends AnnotatedPredicate<ReportPoint> {

  private int cutoffHours;
  private final Counter outOfRangePointTimes;

  public ReportPointTimestampInRangeFilter(final int cutoffHours) {
    this.cutoffHours = cutoffHours;
    this.outOfRangePointTimes = Metrics.newCounter(new MetricName("point", "", "badtime"));
  }

  @Override
  public boolean apply(ReportPoint point) {
    long pointTime = point.getTimestamp();
    long rightNow = System.currentTimeMillis();

    // within <cutoffHours> ago and 1 day ahead
    boolean pointInRange = (pointTime > (rightNow - this.cutoffHours * DateUtils.MILLIS_PER_HOUR)) &&
        (pointTime < (rightNow + DateUtils.MILLIS_PER_DAY));
    if (!pointInRange) {
      outOfRangePointTimes.inc();
    }
    return pointInRange;
  }

  @Override
  public String getMessage(ReportPoint point) {
    return this.apply(point) ? null : "WF-402: Point outside of reasonable timeframe (" + point.toString() + ")";
  }
}
