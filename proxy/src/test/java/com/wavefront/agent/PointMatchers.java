package com.wavefront.agent;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.Map;

import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class PointMatchers {

  private static String mapToString(Map<String, String> map) {
    if (map.size() == 0) return "";
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      sb.append(entry.getKey()).append("=").append(entry.getValue()).append(", ");
    }
    int len = sb.toString().length();
    sb.replace(len - 2, len, "");
    return sb.toString();
  }

  private static <T, S> boolean isSubMap(Map<T, S> m1, Map<T, S> m2) {
    for (Map.Entry<T, S> m1entry : m1.entrySet()) {
      if (!m2.keySet().contains(m1entry.getKey())) return false;
      if (!m2.get(m1entry.getKey()).equals(m1entry.getValue())) return false;
    }
    return true;
  }

  private static <T, S> boolean mapsEqual(Map<T, S> m1, Map<T, S> m2) {
    return isSubMap(m1, m2) && isSubMap(m2, m1);
  }

  public static Matcher<ReportPoint> matches(Object value, String metricName, Map<String, String> tags) {
    return new BaseMatcher<ReportPoint>() {

      @Override
      public boolean matches(Object o) {
        ReportPoint me = (ReportPoint) o;
        return me.getValue().equals(value) && me.getMetric().equals(metricName)
            && mapsEqual(me.getAnnotations(), tags);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(
            "Value should equal " + value.toString() + " and have metric name " + metricName + " and tags "
                + mapToString(tags));

      }
    };
  }

  public static Matcher<ReportPoint> matches(Object value, String metricName, String hostName,
                                             Map<String, String> tags) {
    return new BaseMatcher<ReportPoint>() {

      @Override
      public boolean matches(Object o) {
        ReportPoint me = (ReportPoint) o;
        return me.getValue().equals(value) && me.getMetric().equals(metricName) && me.getHost().equals(hostName)
            && mapsEqual(me.getAnnotations(), tags);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(
            "Value should equal " + value.toString() + " and have metric name " + metricName + ", host " + hostName +
                ", and tags " + mapToString(tags));
      }
    };
  }

  public static Matcher<ReportPoint> almostMatches(double value, String metricName, Map<String, String> tags) {
    return new BaseMatcher<ReportPoint>() {

      @Override
      public boolean matches(Object o) {
        ReportPoint me = (ReportPoint) o;
        double given = (double) me.getValue();
        return Math.abs(value - given) < 0.001
            && me.getMetric().equals(metricName)
            && mapsEqual(me.getAnnotations(), tags);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(
            "Value should approximately equal " + value + " and have metric name " + metricName + " and tags "
                + mapToString(tags));

      }
    };
  }

  public static Matcher<ReportPoint> histogramMatches(int samples, double weight) {
    return new BaseMatcher<ReportPoint>() {

      @Override
      public boolean matches(Object o) {
        ReportPoint point = (ReportPoint) o;
        if (!(point.getValue() instanceof Histogram)) return false;
        Histogram value = (Histogram) point.getValue();
        double sum = 0;
        for (int i = 0; i < value.getBins().size(); i++) {
          sum += value.getBins().get(i) * value.getCounts().get(i);
        }
        return sum == weight && value.getCounts().stream().reduce(Integer::sum).get() == samples;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(
            "Total histogram weight should be " + weight + ", and total samples = " + samples);
      }
    };
  }
}
