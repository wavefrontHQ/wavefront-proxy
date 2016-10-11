package com.wavefront.metrics;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import sunnylabs.report.Histogram;
import sunnylabs.report.ReportPoint;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.truth.Truth.assertThat;
import static com.wavefront.metrics.JsonMetricsParser.report;

/**
 * Unit tests around {@link JsonMetricsParser}
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class JsonMetricsParserTest {
  private final static JsonFactory factory = new JsonFactory(new ObjectMapper());
  private final List<ReportPoint> points = newArrayList();

  @Before
  public void setUp() throws Exception {
    points.clear();
  }

  @Test
  public void testStandardHistogram() throws IOException {
    JsonNode node = factory.createParser(
        "{\"test.metric\":{\"count\":3,\"min\":10.0,\"max\":1000.0,\"mean\":370.0,\"median\":100.0,\"p75\":1000.0,\"p95\":1000.0,\"p99\":1000.0,\"p999\":1000.0}}"
    ).readValueAsTree();
    report("customer", "path", node, points, "host", 100L);

    assertThat(points).hasSize(9);
    assertThat(points.get(0).getMetric()).isEqualTo("path.test.metric.count");
    assertThat(points.get(0).getValue()).isEqualTo(3.0);
    assertThat(points.get(0).getHost()).isEqualTo("host");
    assertThat(points.get(0).getTable()).isEqualTo("customer");
    assertThat(points.get(0).getTimestamp()).isEqualTo(100L);

    assertThat(points.get(1).getMetric()).isEqualTo("path.test.metric.min");
    assertThat(points.get(1).getValue()).isEqualTo(10.0);
    assertThat(points.get(1).getHost()).isEqualTo("host");
    assertThat(points.get(1).getTable()).isEqualTo("customer");
    assertThat(points.get(1).getTimestamp()).isEqualTo(100L);

    assertThat(points.get(2).getMetric()).isEqualTo("path.test.metric.max");
    assertThat(points.get(2).getValue()).isEqualTo(1000.0);
    assertThat(points.get(2).getHost()).isEqualTo("host");
    assertThat(points.get(2).getTable()).isEqualTo("customer");
    assertThat(points.get(2).getTimestamp()).isEqualTo(100L);

    assertThat(points.get(3).getMetric()).isEqualTo("path.test.metric.mean");
    assertThat(points.get(3).getValue()).isEqualTo(370.0);
    assertThat(points.get(3).getHost()).isEqualTo("host");
    assertThat(points.get(3).getTable()).isEqualTo("customer");
    assertThat(points.get(3).getTimestamp()).isEqualTo(100L);

    assertThat(points.get(4).getMetric()).isEqualTo("path.test.metric.median");
    assertThat(points.get(4).getValue()).isEqualTo(100.0);
    assertThat(points.get(4).getHost()).isEqualTo("host");
    assertThat(points.get(4).getTable()).isEqualTo("customer");
    assertThat(points.get(4).getTimestamp()).isEqualTo(100L);

    assertThat(points.get(5).getMetric()).isEqualTo("path.test.metric.p75");
    assertThat(points.get(5).getValue()).isEqualTo(1000.0);
    assertThat(points.get(5).getHost()).isEqualTo("host");
    assertThat(points.get(5).getTable()).isEqualTo("customer");
    assertThat(points.get(5).getTimestamp()).isEqualTo(100L);

    assertThat(points.get(6).getMetric()).isEqualTo("path.test.metric.p95");
    assertThat(points.get(6).getValue()).isEqualTo(1000.0);
    assertThat(points.get(6).getHost()).isEqualTo("host");
    assertThat(points.get(6).getTable()).isEqualTo("customer");
    assertThat(points.get(6).getTimestamp()).isEqualTo(100L);

    assertThat(points.get(7).getMetric()).isEqualTo("path.test.metric.p99");
    assertThat(points.get(7).getValue()).isEqualTo(1000.0);
    assertThat(points.get(7).getHost()).isEqualTo("host");
    assertThat(points.get(7).getTable()).isEqualTo("customer");
    assertThat(points.get(7).getTimestamp()).isEqualTo(100L);

    assertThat(points.get(8).getMetric()).isEqualTo("path.test.metric.p999");
    assertThat(points.get(8).getValue()).isEqualTo(1000.0);
    assertThat(points.get(8).getHost()).isEqualTo("host");
    assertThat(points.get(8).getTable()).isEqualTo("customer");
    assertThat(points.get(8).getTimestamp()).isEqualTo(100L);
  }

  @Test
  public void testWavefrontHistogram() throws IOException {
    JsonNode node = factory.createParser("{\"test.metric\":{\"bins\":[{\"count\":2,\"startMillis\":0,\"durationMillis\":60000,\"means\":[10.0,100.0],\"counts\":[1,1]},{\"count\":1,\"startMillis\":60000,\"durationMillis\":60000,\"means\":[1000.0],\"counts\":[1]}]}}").readValueAsTree();
    report("customer", "path", node, points, "host", 100L);

    assertThat(points).hasSize(2);

    assertThat(points.get(0).getMetric()).isEqualTo("path.test.metric");
    assertThat(points.get(0).getValue()).isInstanceOf(Histogram.class);
    assertThat(((Histogram) points.get(0).getValue()).getDuration()).isEqualTo(60000L);
    assertThat(((Histogram) points.get(0).getValue()).getBins()).isEqualTo(newArrayList(10.0, 100.0));
    assertThat(((Histogram) points.get(0).getValue()).getCounts()).isEqualTo(newArrayList(1, 1));
    assertThat(points.get(0).getHost()).isEqualTo("host");
    assertThat(points.get(0).getTable()).isEqualTo("customer");
    assertThat(points.get(0).getTimestamp()).isEqualTo(0L);

    assertThat(points.get(1).getMetric()).isEqualTo("path.test.metric");
    assertThat(points.get(1).getValue()).isInstanceOf(Histogram.class);
    assertThat(((Histogram) points.get(1).getValue()).getDuration()).isEqualTo(60000L);
    assertThat(((Histogram) points.get(1).getValue()).getBins()).isEqualTo(newArrayList(1000.0));
    assertThat(((Histogram) points.get(1).getValue()).getCounts()).isEqualTo(newArrayList(1));
    assertThat(points.get(1).getHost()).isEqualTo("host");
    assertThat(points.get(1).getTable()).isEqualTo("customer");
    assertThat(points.get(1).getTimestamp()).isEqualTo(60000L);
  }
}