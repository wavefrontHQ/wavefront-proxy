package com.wavefront.agent;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.Map;

import sunnylabs.report.ReportPoint;

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

}
