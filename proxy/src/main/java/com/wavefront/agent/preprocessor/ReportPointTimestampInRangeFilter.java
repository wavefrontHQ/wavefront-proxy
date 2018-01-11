package com.wavefront.agent.preprocessor;

import com.wavefront.common.Clock;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.time.DateUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import wavefront.report.ReportPoint;

/**
 * Filter condition for valid timestamp - should be no more than 1 day in the future
 * and no more than X hours (usually 8760, or 1 year) in the past
 *
 * Created by Vasily on 9/16/16.
 * Updated by Howard on 1/10/18 to add support for preCutOffHours
 */
public class ReportPointTimestampInRangeFilter extends AnnotatedPredicate<ReportPoint> {

  private final int backCutoffHours;
  private final int preCutoffHours;

  private final Counter outOfRangePointTimes;
  @Nullable
  private String message = null;

  public ReportPointTimestampInRangeFilter(final int backCutoffHours, final int preCutoffHours) {
    this.backCutoffHours = backCutoffHours;
    this.preCutoffHours = preCutoffHours;
    this.outOfRangePointTimes = Metrics.newCounter(new MetricName("point", "", "badtime"));
  }

  @Override
  public boolean apply(@NotNull ReportPoint point) {
    this.message = null;
    long pointTime = point.getTimestamp();
    long rightNow = Clock.now();

    // within <backCutoffHours> ago and within <preCutoffHours>
    boolean pointInRange = (pointTime > (rightNow - this.backCutoffHours * DateUtils.MILLIS_PER_HOUR)) &&
        (pointTime < (rightNow + (this.preCutoffHours * DateUtils.MILLIS_PER_HOUR));
    if (!pointInRange) {
      outOfRangePointTimes.inc();
      this.message = "WF-402: Point outside of reasonable timeframe (" + point.toString() + ")";
    }
    return pointInRange;
  }

  @Override
  public String getMessage(ReportPoint point) {
    return this.message;
  }
}
