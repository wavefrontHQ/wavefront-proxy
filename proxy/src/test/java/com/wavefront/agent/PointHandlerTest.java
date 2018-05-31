package com.wavefront.agent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.time.DateUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import wavefront.report.Histogram;
import wavefront.report.HistogramType;
import wavefront.report.ReportPoint;

import static com.google.common.truth.Truth.assertThat;

/**
 * @author Andrew Kao (andrew@wavefront.com), Jason Bau (jbau@wavefront.com)
 */
public class PointHandlerTest {
  private ReportPoint histogramPoint;

  @Before
  public void setUp() {
    Histogram h = Histogram.newBuilder()
        .setType(HistogramType.TDIGEST)
        .setBins(ImmutableList.of(10D, 20D))
        .setCounts(ImmutableList.of(2, 4))
        .setDuration((int) DateUtils.MILLIS_PER_MINUTE)
        .build();
    histogramPoint = ReportPoint.newBuilder()
        .setTable("customer")
        .setValue(h)
        .setMetric("TestMetric")
        .setHost("TestSource")
        .setTimestamp(1469751813000L)
        .setAnnotations(ImmutableMap.of("keyA", "valueA", "keyB", "valueB"))
        .build();
  }

  @Test
  public void testPointIllegalChars() {
    String input = "metric1";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good.metric2";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good-metric3";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good_metric4";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good,metric5";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good/metric6";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // first character can no longer be ~
    input = "~good.metric7";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // first character can be ∆ (\u2206)
    input = "∆delta.metric8";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // first character can be Δ (\u0394)
    input = "Δdelta.metric9";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // non-first character cannot be ~
    input = "~good.~metric";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // non-first character cannot be ∆ (\u2206)
    input = "∆delta.∆metric";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // non-first character cannot be Δ (\u0394)
    input = "∆delta.Δmetric";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // cannot end in ~
    input = "good.metric.~";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // cannot end in ∆ (\u2206)
    input = "delta.metric.∆";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // cannot end in Δ (\u0394)
    input = "delta.metric.Δ";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "abcdefghijklmnopqrstuvwxyz.0123456789,/_-ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "abcdefghijklmnopqrstuvwxyz.0123456789,/_-ABCDEFGHIJKLMNOPQRSTUVWXYZ~";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as;df";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as:df";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as df";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as'df";
    Assert.assertFalse(Validation.charactersAreValid(input));
  }

  @Test
  public void testPointAnnotationKeyValidation() {
    Map<String, String> goodMap = new HashMap<String, String>();
    goodMap.put("key", "value");

    Map<String, String> badMap = new HashMap<String, String>();
    badMap.put("k:ey", "value");

    ReportPoint rp = new ReportPoint("some metric", System.currentTimeMillis(), 10L, "host", "table",
        goodMap);
    Assert.assertTrue(Validation.annotationKeysAreValid(rp));

    rp.setAnnotations(badMap);
    Assert.assertFalse(Validation.annotationKeysAreValid(rp));
  }

  @Test
  public void testReportPointToString() {
    // Common case.
    Assert.assertEquals("\"some metric\" 10 1469751813 source=\"host\" \"foo\"=\"bar\" \"boo\"=\"baz\"",
        PointHandlerImpl.pointToString(new ReportPoint("some metric",1469751813000L, 10L, "host", "table",
            ImmutableMap.of("foo", "bar", "boo", "baz"))));
    Assert.assertEquals("\"some metric\" 10 1469751813 source=\"host\"",
        PointHandlerImpl.pointToString(new ReportPoint("some metric",1469751813000L, 10L, "host", "table",
            ImmutableMap.of())));

    // Quote in metric name
    Assert.assertEquals("\"some\\\"metric\" 10 1469751813 source=\"host\"",
        PointHandlerImpl.pointToString(new ReportPoint("some\"metric", 1469751813000L, 10L, "host", "table",
            new HashMap<String, String>()))
    );
    // Quote in tags
    Assert.assertEquals("\"some metric\" 10 1469751813 source=\"host\" \"foo\\\"\"=\"\\\"bar\" \"bo\\\"o\"=\"baz\"",
        PointHandlerImpl.pointToString(new ReportPoint("some metric", 1469751813000L, 10L, "host", "table",
            ImmutableMap.of("foo\"", "\"bar", "bo\"o", "baz")))
    );
  }

  @Test
  public void testReportPointToString_stringValue() {
    histogramPoint.setValue("Test");

    String subject = PointHandlerImpl.pointToString(histogramPoint);
    assertThat(subject).isEqualTo("\"TestMetric\" Test 1469751813 source=\"TestSource\" \"keyA\"=\"valueA\" \"keyB\"=\"valueB\"");
  }


  @Test
  public void testHistogramReportPointToString() {
    String subject = PointHandlerImpl.pointToString(histogramPoint);

    assertThat(subject).isEqualTo("!M 1469751813 #2 10.0 #4 20.0 \"TestMetric\" source=\"TestSource\" \"keyA\"=\"valueA\" \"keyB\"=\"valueB\"");
  }

  @Test(expected = RuntimeException.class)
  public void testHistogramReportPointToString_unsupportedDuration() {
    ((Histogram)histogramPoint.getValue()).setDuration(13);

    PointHandlerImpl.pointToString(histogramPoint);
  }

  @Test
  public void testHistogramReportPointToString_binCountMismatch() {
    ((Histogram)histogramPoint.getValue()).setCounts(ImmutableList.of(10));

    String subject = PointHandlerImpl.pointToString(histogramPoint);
    assertThat(subject).isEqualTo("!M 1469751813 #10 10.0 \"TestMetric\" source=\"TestSource\" \"keyA\"=\"valueA\" \"keyB\"=\"valueB\"");
  }

  @Test
  public void testHistogramReportPointToString_quotesInMetric() {
    histogramPoint.setMetric("Test\"Metric");

    String subject = PointHandlerImpl.pointToString(histogramPoint);
    assertThat(subject).isEqualTo("!M 1469751813 #2 10.0 #4 20.0 \"Test\\\"Metric\" source=\"TestSource\" \"keyA\"=\"valueA\" \"keyB\"=\"valueB\"");
  }

  @Test
  public void testHistogramReportPointToString_quotesInTags() {
    histogramPoint.setAnnotations(ImmutableMap.of("K\"ey", "V\"alue"));

    String subject = PointHandlerImpl.pointToString(histogramPoint);
    assertThat(subject).isEqualTo("!M 1469751813 #2 10.0 #4 20.0 \"TestMetric\" source=\"TestSource\" \"K\\\"ey\"=\"V\\\"alue\"");
  }

  @Test(expected = RuntimeException.class)
  public void testHistogramReportPointToString_BadValue() {
    ReportPoint p = new ReportPoint("m", 1469751813L, new ArrayUtils(), "h", "c", ImmutableMap.of());

    PointHandlerImpl.pointToString(p);
  }
}
