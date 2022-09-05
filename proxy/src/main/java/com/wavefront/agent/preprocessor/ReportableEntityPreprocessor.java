package com.wavefront.agent.preprocessor;

import javax.annotation.Nonnull;
import wavefront.report.ReportLog;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

/**
 * A container class for multiple types of rules (point line-specific and parsed entity-specific)
 *
 * <p>Created by Vasily on 9/15/16.
 */
public class ReportableEntityPreprocessor {

  private final Preprocessor<String> pointLinePreprocessor;
  private final Preprocessor<ReportPoint> reportPointPreprocessor;
  private final Preprocessor<Span> spanPreprocessor;
  private final Preprocessor<ReportLog> reportLogPreprocessor;

  public ReportableEntityPreprocessor() {
    this(new Preprocessor<>(), new Preprocessor<>(), new Preprocessor<>(), new Preprocessor<>());
  }

  private ReportableEntityPreprocessor(
      @Nonnull Preprocessor<String> pointLinePreprocessor,
      @Nonnull Preprocessor<ReportPoint> reportPointPreprocessor,
      @Nonnull Preprocessor<Span> spanPreprocessor,
      @Nonnull Preprocessor<ReportLog> reportLogPreprocessor) {
    this.pointLinePreprocessor = pointLinePreprocessor;
    this.reportPointPreprocessor = reportPointPreprocessor;
    this.spanPreprocessor = spanPreprocessor;
    this.reportLogPreprocessor = reportLogPreprocessor;
  }

  // TODO(amitw): We will need to add something like this for logs this for log to handle json
  // log instead of a line
  public Preprocessor<String> forPointLine() {
    return pointLinePreprocessor;
  }

  public Preprocessor<ReportPoint> forReportPoint() {
    return reportPointPreprocessor;
  }

  public Preprocessor<Span> forSpan() {
    return spanPreprocessor;
  }

  public Preprocessor<ReportLog> forReportLog() {
    return reportLogPreprocessor;
  }

  public ReportableEntityPreprocessor merge(ReportableEntityPreprocessor other) {
    return new ReportableEntityPreprocessor(
        this.pointLinePreprocessor.merge(other.forPointLine()),
        this.reportPointPreprocessor.merge(other.forReportPoint()),
        this.spanPreprocessor.merge(other.forSpan()),
        this.reportLogPreprocessor.merge(other.forReportLog()));
  }
}
