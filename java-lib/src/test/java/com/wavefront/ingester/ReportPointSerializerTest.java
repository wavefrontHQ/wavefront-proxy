package com.wavefront.ingester;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.time.DateUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.function.Function;

import wavefront.report.Histogram;
import wavefront.report.HistogramType;
import wavefront.report.ReportPoint;

import static com.google.common.truth.Truth.assertThat;

/**
 * @author Andrew Kao (andrew@wavefront.com), Jason Bau (jbau@wavefront.com), vasily@wavefront.com
 */
public class ReportPointSerializerTest {
  private ReportPoint histogramPoint;
  
  private Function<ReportPoint, String> serializer = new ReportPointSerializer();

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
  public void testReportPointToString() {
    // Common case.
    Assert.assertEquals("\"some metric\" 10 1469751813 source=\"host\" \"foo\"=\"bar\" \"boo\"=\"baz\"",
        serializer.apply(new ReportPoint("some metric",1469751813000L, 10L, "host", "table",
            ImmutableMap.of("foo", "bar", "boo", "baz"))));
    Assert.assertEquals("\"some metric\" 10 1469751813 source=\"host\"",
        serializer.apply(new ReportPoint("some metric",1469751813000L, 10L, "host", "table",
            ImmutableMap.of())));

    // Quote in metric name
    Assert.assertEquals("\"some\\\"metric\" 10 1469751813 source=\"host\"",
        serializer.apply(new ReportPoint("some\"metric", 1469751813000L, 10L, "host", "table",
            new HashMap<String, String>()))
    );
    // Quote in tags
    Assert.assertEquals("\"some metric\" 10 1469751813 source=\"host\" \"foo\\\"\"=\"\\\"bar\" \"bo\\\"o\"=\"baz\"",
        serializer.apply(new ReportPoint("some metric", 1469751813000L, 10L, "host", "table",
            ImmutableMap.of("foo\"", "\"bar", "bo\"o", "baz")))
    );
  }

  @Test
  public void testReportPointToString_stringValue() {
    histogramPoint.setValue("Test");

    String subject = serializer.apply(histogramPoint);
    assertThat(subject).isEqualTo("\"TestMetric\" Test 1469751813 source=\"TestSource\" \"keyA\"=\"valueA\" \"keyB\"=\"valueB\"");
  }


  @Test
  public void testHistogramReportPointToString() {
    String subject = serializer.apply(histogramPoint);

    assertThat(subject).isEqualTo("!M 1469751813 #2 10.0 #4 20.0 \"TestMetric\" source=\"TestSource\" \"keyA\"=\"valueA\" \"keyB\"=\"valueB\"");
  }

  @Test(expected = RuntimeException.class)
  public void testHistogramReportPointToString_unsupportedDuration() {
    ((Histogram)histogramPoint.getValue()).setDuration(13);

    serializer.apply(histogramPoint);
  }

  @Test
  public void testHistogramReportPointToString_binCountMismatch() {
    ((Histogram)histogramPoint.getValue()).setCounts(ImmutableList.of(10));

    String subject = serializer.apply(histogramPoint);
    assertThat(subject).isEqualTo("!M 1469751813 #10 10.0 \"TestMetric\" source=\"TestSource\" \"keyA\"=\"valueA\" \"keyB\"=\"valueB\"");
  }

  @Test
  public void testHistogramReportPointToString_quotesInMetric() {
    histogramPoint.setMetric("Test\"Metric");

    String subject = serializer.apply(histogramPoint);
    assertThat(subject).isEqualTo("!M 1469751813 #2 10.0 #4 20.0 \"Test\\\"Metric\" source=\"TestSource\" \"keyA\"=\"valueA\" \"keyB\"=\"valueB\"");
  }

  @Test
  public void testHistogramReportPointToString_quotesInTags() {
    histogramPoint.setAnnotations(ImmutableMap.of("K\"ey", "V\"alue"));

    String subject = serializer.apply(histogramPoint);
    assertThat(subject).isEqualTo("!M 1469751813 #2 10.0 #4 20.0 \"TestMetric\" source=\"TestSource\" \"K\\\"ey\"=\"V\\\"alue\"");
  }

  @Test(expected = RuntimeException.class)
  public void testHistogramReportPointToString_BadValue() {
    ReportPoint p = new ReportPoint("m", 1469751813L, new ArrayUtils(), "h", "c", ImmutableMap.of());

    serializer.apply(p);
  }
}
