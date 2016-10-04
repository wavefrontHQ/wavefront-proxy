package com.wavefront.agent.histogram;

import java.util.concurrent.TimeUnit;

import sunnylabs.report.Histogram;
import sunnylabs.report.ReportPoint;

import static com.google.common.truth.Truth.assertThat;
import static com.wavefront.agent.histogram.Utils.*;

/**
 * Shared test helpers around histograms
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public final class TestUtils {
  private TestUtils() {
    // final abstract...
  }

  public static long DEFAULT_TIME_MILLIS =
      TimeUnit.MINUTES.toMillis(TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis()));
  public static double DEFAULT_VALUE = 1D;

  public static HistogramKey makeKey(String metric) {
    return Utils.makeKey(
        ReportPoint.newBuilder().setMetric(metric).setTimestamp(DEFAULT_TIME_MILLIS).setValue(DEFAULT_VALUE).build(),
        Granularity.MINUTE);
  }


  static void testKeyPointMatch(HistogramKey key, ReportPoint point) {
    assertThat(key).isNotNull();
    assertThat(point).isNotNull();
    assertThat(point.getValue()).isNotNull();
    assertThat(point.getValue() instanceof Histogram).isTrue();

    assertThat(key.getMetric()).isEqualTo(point.getMetric());
    assertThat(key.getSource()).isEqualTo(point.getHost());
    assertThat(key.getTagsAsMap()).isEqualTo(point.getAnnotations());
    assertThat(key.getBinTimeMillis()).isEqualTo(point.getTimestamp());
    assertThat(key.getBinDurationInMillis()).isEqualTo(((Histogram)point.getValue()).getDuration());
  }
}
