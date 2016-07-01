package com.wavefront.agent;

import java.util.List;

import sunnylabs.report.ReportPoint;

/**
 * Interface for a handler of Report Points.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public interface PointHandler {
  /**
   * Send a point for reporting.
   *
   * @param point     Point to report.
   * @param debugLine Debug information to print to console when the line is rejected.
   */
  void reportPoint(ReportPoint point, String debugLine);

  /**
   * Send a collection of points for reporting.
   *
   * @param points Points to report.
   */
  void reportPoints(List<ReportPoint> points);

  /**
   * Called when a blocked line is encountered.
   *
   * @param pointLine Line encountered.
   */
  void handleBlockedPoint(String pointLine);
}
