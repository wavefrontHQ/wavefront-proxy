package com.wavefront.agent.histogram;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.concurrent.TimeUnit;
import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

/** Shared test helpers around histograms */
public final class TestUtils {
  public static long DEFAULT_TIME_MILLIS =
      TimeUnit.MINUTES.toMillis(TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis()));
  public static double DEFAULT_VALUE = 1D;

  private TestUtils() {
    // final abstract...
  }

  /**
   * Creates a histogram accumulation key for given metric at minute granularity and
   * DEFAULT_TIME_MILLIS
   */
  public static HistogramKey makeKey(String metric) {
    return makeKey(metric, Granularity.MINUTE);
  }

  /**
   * Creates a histogram accumulation key for a given metric and granularity around
   * DEFAULT_TIME_MILLIS
   */
  public static HistogramKey makeKey(String metric, Granularity granularity) {
    return HistogramUtils.makeKey(
        ReportPoint.newBuilder()
            .setMetric(metric)
            .setAnnotations(ImmutableMap.of("tagk", "tagv"))
            .setTimestamp(DEFAULT_TIME_MILLIS)
            .setValue(DEFAULT_VALUE)
            .build(),
        granularity);
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
    assertThat(key.getBinDurationInMillis())
        .isEqualTo(((Histogram) point.getValue()).getDuration());
  }
}
