package com.wavefront.agent.preprocessor;

import wavefront.report.ReportPoint;

/**
 * A container class for both types of rules (point line-specific and parsed point-specific)
 *
 * Created by Vasily on 9/15/16.
 */
public class PointPreprocessor {

  private final Preprocessor<String> pointLinePreprocessor = new Preprocessor<>();
  private final Preprocessor<ReportPoint> reportPointPreprocessor = new Preprocessor<>();

  public Preprocessor<String> forPointLine() {
    return pointLinePreprocessor;
  }

  public Preprocessor<ReportPoint> forReportPoint() {
    return reportPointPreprocessor;
  }
}
