package com.wavefront.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link WavefrontDataFormat}
 *
 * @author Clement Pang (clement@wavefront.com).
 * @author Vikram Raman (vikram@wavefront.com).
 * @author Han Zhang (zhanghan@vmware.com).
 */
public class WavefrontDataFormatTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testPointToString() {
    assertEquals("\"name\" 10.0 1469751813 source=\"source\" \"foo\"=\"bar\" \"bar\"=\"foo\"",
        WavefrontDataFormat.pointToString("name",10L, 1469751813L, "source",
            ImmutableMap.of("foo", "bar", "bar", "foo"), false));
  }

  @Test
  public void testPointWithoutName() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("metric name cannot be blank"));
    assertNull(WavefrontDataFormat.pointToString(null, 0.0, null, "source", null, false));
  }

  @Test
  public void testPointWithoutSource() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("source cannot be blank"));
    assertNull(WavefrontDataFormat.pointToString("name", 0.0, null, null, null, false));
  }

  @Test
  public void testHistogramToStrings() {

    Set<HistogramGranularity> histogramGranularities = new HashSet<>();
    histogramGranularities.add(HistogramGranularity.MINUTE);

    List<Pair<Double, Integer>> centroids = new ArrayList<>();
    centroids.add(new Pair<>(1.23, 5));

    List<String> actual = WavefrontDataFormat.histogramToStrings(histogramGranularities, null, centroids, "name",
        "source", null, false);

    assertThat(actual, CoreMatchers.hasItems("!M #5 1.23 \"name\" source=\"source\""));
    assertEquals(1, actual.size());

    histogramGranularities.add(HistogramGranularity.HOUR);
    histogramGranularities.add(HistogramGranularity.DAY);
    centroids.add(new Pair<>(10.0, 1));
    centroids.add(new Pair<>(456346.3453453451, 501));
    centroids.add(new Pair<>(1.23, 1));
    centroids.add(new Pair<>(-9.3, 12));

    actual = WavefrontDataFormat.histogramToStrings(histogramGranularities, 1532456268L, centroids,
        "name", "source", ImmutableMap.of("foo", "bar", "bar", "foo"), true);

    assertThat(actual, CoreMatchers.hasItems(
        "!M 1532456268 #5 1.23 #1 10.0 #501 456346.3453453451 #1 1.23 #12 -9.3 \"name\" source=\"source\" \"foo\"=\"bar\" \"bar\"=\"foo\"\n",
        "!H 1532456268 #5 1.23 #1 10.0 #501 456346.3453453451 #1 1.23 #12 -9.3 \"name\" source=\"source\" \"foo\"=\"bar\" \"bar\"=\"foo\"\n",
        "!D 1532456268 #5 1.23 #1 10.0 #501 456346.3453453451 #1 1.23 #12 -9.3 \"name\" source=\"source\" \"foo\"=\"bar\" \"bar\"=\"foo\"\n"
    ));
    assertEquals(3, actual.size());
  }

  @Test
  public void testHistogramWithoutGranularity() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("histogram granularities cannot be null or empty"));
    WavefrontDataFormat.histogramToStrings(new HashSet<>(), null, ImmutableList.of(new Pair<>(1.23, 5)), "name",
        "source", null, false);
  }

  @Test
  public void testHistogramWithoutCentroids() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("centroids cannot be null or empty"));
    WavefrontDataFormat.histogramToStrings(ImmutableSet.of(HistogramGranularity.MINUTE), null, new ArrayList<>(),
        "name", "source", null, false);
  }

  @Test
  public void testHistogramWithoutName() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("distribution name cannot be blank"));
    WavefrontDataFormat.histogramToStrings(ImmutableSet.of(HistogramGranularity.MINUTE), null, ImmutableList.of(new
        Pair<>(1.23, 5)), null, "source", null, false);
  }

  @Test
  public void testHistogramWithoutSource() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("source cannot be blank"));
    WavefrontDataFormat.histogramToStrings(ImmutableSet.of(HistogramGranularity.MINUTE), null, ImmutableList.of(new
        Pair<>(1.23, 5)), "name", null, null, false);
  }

  @Test
  public void testHistogramWithNullPair() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("centroid cannot be null"));
    List<Pair<Double, Integer>> centroids = new ArrayList<>();
    centroids.add(null);
    WavefrontDataFormat.histogramToStrings(ImmutableSet.of(HistogramGranularity.MINUTE), null, centroids,
        "name", "source", null, false);
  }

  @Test
  public void testHistogramWithNullValue() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("centroid value cannot be null"));
    WavefrontDataFormat.histogramToStrings(ImmutableSet.of(HistogramGranularity.MINUTE), null, ImmutableList.of(new
        Pair<>(null, 5)), "name", "source", null, false);
  }

  @Test
  public void testHistogramWithNullCount() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("centroid count cannot be null"));
    WavefrontDataFormat.histogramToStrings(ImmutableSet.of(HistogramGranularity.MINUTE), null, ImmutableList.of(new
        Pair<>(1.23, null)), "name", "source", null, false);
  }

  @Test
  public void testHistogramWithNonPositiveCount() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("centroid count cannot be less than 1"));
    WavefrontDataFormat.histogramToStrings(ImmutableSet.of(HistogramGranularity.MINUTE), null, ImmutableList.of(new
        Pair<>(1.23, 0)), "name", "source", null, false);
  }

  @Test
  public void testPointTagsToString() {
    assertEquals(" \"foo\"=\"bar\" \"bar\"=\"foo\"", WavefrontDataFormat.pointTagsToString(ImmutableMap.of("foo",
        "bar", "bar", "foo")));
  }

  @Test
  public void testPointTagsWithNullTag() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("point tag key cannot be blank"));
    Map<String, String> pointTags = new HashMap<>();
    pointTags.put(null, "bar");
    WavefrontDataFormat.pointTagsToString(pointTags);
  }

  @Test
  public void testPointTagsWithNullValue() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is("point tag value cannot be blank"));
    Map<String, String> pointTags = new HashMap<>();
    pointTags.put("foo", null);
    WavefrontDataFormat.pointTagsToString(pointTags);
  }

  @Test
  public void testSanitize() {
    assertEquals("\"hello\"", WavefrontDataFormat.sanitize("hello"));
    assertEquals("\"hello-world\"", WavefrontDataFormat.sanitize("hello world"));
    assertEquals("\"hello.world\"", WavefrontDataFormat.sanitize("hello.world"));
    assertEquals("\"hello\\\"world\\\"\"", WavefrontDataFormat.sanitize("hello\"world\""));
    assertEquals("\"hello'world\"", WavefrontDataFormat.sanitize("hello'world"));
  }
}
