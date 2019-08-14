package com.wavefront.agent;

import java.util.List;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * Interface for a handler of Report Points.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
@Deprecated
public interface PointHandler {
  /**
   * Send a point for reporting.
   *
   * @param point     Point to report.
   * @param debugLine Debug information to print to console when the line is rejected.
   *                  If null, then use the entire point converted to string.
   */
  void reportPoint(ReportPoint point, @Nullable String debugLine);

  /**
   * Send a collection of points for reporting.
   *
   * @param points Points to report.
   */
  void reportPoints(List<ReportPoint> points);

  /**
   * Called when a blocked line is encountered.
   *
   * @param pointLine Line encountered. If null, it will increment the blocked points counter
   *                  but won't write to the log
   */
  void handleBlockedPoint(@Nullable String pointLine);
}
