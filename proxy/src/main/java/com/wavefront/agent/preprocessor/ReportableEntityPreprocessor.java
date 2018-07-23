package com.wavefront.agent.preprocessor;

import com.wavefront.agent.handlers.ReportableEntityHandler;

import wavefront.report.ReportPoint;
import wavefront.report.Span;

/**
 * A container class for multiple types of rules (point line-specific and parsed entity-specific)
 *
 * Created by Vasily on 9/15/16.
 */
public class ReportableEntityPreprocessor {

  private final Preprocessor<String> pointLinePreprocessor = new Preprocessor<>();
  private final Preprocessor<ReportPoint> reportPointPreprocessor = new Preprocessor<>();
  private final Preprocessor<Span> spanPreprocessor = new Preprocessor<>();

  public Preprocessor<String> forPointLine() {
    return pointLinePreprocessor;
  }

  public Preprocessor<ReportPoint> forReportPoint() {
    return reportPointPreprocessor;
  }

  public Preprocessor<Span> forSpan() {
    return spanPreprocessor;
  }

  public boolean preprocessPointLine(String pointLine, ReportableEntityHandler handler) {
    pointLine = pointLinePreprocessor.transform(pointLine);

    // apply white/black lists after formatting
    if (!pointLinePreprocessor.filter(pointLine)) {
      if (pointLinePreprocessor.getLastFilterResult() != null) {
        handler.reject(pointLine, pointLinePreprocessor.getLastFilterResult());
      } else {
        handler.block(pointLine);
      }
      return false;
    }
    return true;
  }
}
