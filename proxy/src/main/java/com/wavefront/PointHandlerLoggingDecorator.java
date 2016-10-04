package com.wavefront;

import com.wavefront.agent.PointHandler;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import sunnylabs.report.Histogram;
import sunnylabs.report.ReportPoint;

import static java.util.logging.Level.INFO;

/**
 * Stores basic stats around logged points. Exposes a runnable to log them.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class PointHandlerLoggingDecorator implements PointHandler {
  private final static Logger logger = Logger.getLogger(PointHandlerLoggingDecorator.class.getCanonicalName());

  private final AtomicLong numScalarPointsReported;
  private final AtomicLong numHistogramPointsReported;
  private final AtomicLong numHistogramSamplesReported;
  private final AtomicLong numBlockedPointsReported;
  private final PointHandler delegate;

  public PointHandlerLoggingDecorator(PointHandler delegate) {
    this.delegate = delegate;
    numScalarPointsReported = new AtomicLong(0);
    numHistogramPointsReported = new AtomicLong(0);
    numHistogramSamplesReported = new AtomicLong(0);
    numBlockedPointsReported = new AtomicLong(0);
  }

  private void countPoint(ReportPoint point) {
    if (point.getValue() instanceof Histogram) {
      Histogram h = (Histogram) point.getValue();
      numHistogramPointsReported.addAndGet(1);
      numHistogramSamplesReported.addAndGet(h.getCounts().stream().mapToLong(Integer::longValue).sum());
    } else {
      numScalarPointsReported.addAndGet(1);
    }
  }

  @Override
  public void reportPoint(ReportPoint point, String debugLine) {
    countPoint(point);
    delegate.reportPoint(point, debugLine);
  }

  @Override
  public void reportPoints(List<ReportPoint> points) {
    for (ReportPoint point : points) {
      countPoint(point);
    }
    delegate.reportPoints(points);
  }

  @Override
  public void handleBlockedPoint(String pointLine) {
    numBlockedPointsReported.addAndGet(1);
    delegate.handleBlockedPoint(pointLine);
  }

  @Override
  public String toString() {
    return "PointHandlerLoggingDecorator{" +
        "numScalarPointsReported=" + numScalarPointsReported +
        ", numHistogramPointsReported=" + numHistogramPointsReported +
        ", numHistogramSamplesReported=" + numHistogramSamplesReported +
        ", numBlockedPointsReported=" + numBlockedPointsReported +
        ", delegate=" + delegate +
        '}';
  }

  public Runnable getLoggingTask() {
    return this::log;
  }

  private void log() {
    logger.log(INFO,
        "PointHandler #scalar: " + numScalarPointsReported.get() +
            ", #hist: " + numHistogramPointsReported.get() +
            ", #histSamples: " + numHistogramSamplesReported.get() +
            ", #blocked: " + numBlockedPointsReported.get());
  }


}
