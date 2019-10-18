package com.wavefront.agent.preprocessor;

import com.wavefront.agent.handlers.ReportableEntityHandler;

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

  public boolean preprocessPointLine(String pointLine, ReportableEntityHandler handler) {
    pointLine = pointLinePreprocessor.transform(pointLine);
    String[] messageHolder = new String[1];

    // apply white/black lists after formatting
    if (!pointLinePreprocessor.filter(pointLine, messageHolder)) {
      if (messageHolder[0] != null) {
        handler.reject(pointLine, messageHolder[0]);
      } else {
        handler.block(pointLine);
      }
      return false;
    }
    return true;
  }

  public ReportableEntityPreprocessor merge(ReportableEntityPreprocessor other) {
    return new ReportableEntityPreprocessor(this.pointLinePreprocessor.merge(other.forPointLine()),
        this.reportPointPreprocessor.merge(other.forReportPoint()),
        this.spanPreprocessor.merge(other.forSpan()));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    ReportableEntityPreprocessor that = (ReportableEntityPreprocessor) obj;
    return this.pointLinePreprocessor.equals(that.forPointLine()) &&
        this.reportPointPreprocessor.equals(that.forReportPoint()) &&
        this.spanPreprocessor.equals(that.forSpan());
  }

  @Override
  public int hashCode() {
    int result = pointLinePreprocessor.hashCode();
    result = 31 * result + reportPointPreprocessor.hashCode();
    result = 31 * result + spanPreprocessor.hashCode();
    return result;
  }
}
