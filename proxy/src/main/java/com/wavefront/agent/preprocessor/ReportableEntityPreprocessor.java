package com.wavefront.agent.preprocessor;

import javax.annotation.Nonnull;

import wavefront.report.ReportPoint;
import wavefront.report.Span;

/**
 * A container class for multiple types of rules (point line-specific and parsed entity-specific)
 *
 * Created by Vasily on 9/15/16.
 */
public class ReportableEntityPreprocessor {

  private final Preprocessor<String> pointLinePreprocessor;
  private final Preprocessor<ReportPoint> reportPointPreprocessor;
  private final Preprocessor<Span> spanPreprocessor;

  public ReportableEntityPreprocessor() {
    this(new Preprocessor<>(), new Preprocessor<>(), new Preprocessor<>());
  }

  private ReportableEntityPreprocessor(@Nonnull Preprocessor<String> pointLinePreprocessor,
                                       @Nonnull Preprocessor<ReportPoint> reportPointPreprocessor,
                                       @Nonnull Preprocessor<Span> spanPreprocessor) {
    this.pointLinePreprocessor = pointLinePreprocessor;
    this.reportPointPreprocessor = reportPointPreprocessor;
    this.spanPreprocessor = spanPreprocessor;
  }

  public Preprocessor<String> forPointLine() {
    return pointLinePreprocessor;
  }

  public Preprocessor<ReportPoint> forReportPoint() {
    return reportPointPreprocessor;
  }

  public Preprocessor<Span> forSpan() {
    return spanPreprocessor;
  }

  public ReportableEntityPreprocessor merge(ReportableEntityPreprocessor other) {
    return new ReportableEntityPreprocessor(this.pointLinePreprocessor.merge(other.forPointLine()),
        this.reportPointPreprocessor.merge(other.forReportPoint()),
        this.spanPreprocessor.merge(other.forSpan()));
  }
}
