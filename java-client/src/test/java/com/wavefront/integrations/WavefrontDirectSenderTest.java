package com.wavefront.integrations;

import com.google.common.collect.ImmutableMap;

import com.wavefront.common.HistogramGranularity;
import com.wavefront.common.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNull;

/**
 * Tests for {@link WavefrontDirectSender}
 *
 * @author Vikram Raman (vikram@wavefront.com).
 */
public class WavefrontDirectSenderTest {

  @Test
  public void testPointToString() {
    assertNull(WavefrontDirectSender.pointToString(null, 0.0, null, "source", null));
    assertNull(WavefrontDirectSender.pointToString("name", 0.0, null, null, null));

    Assert.assertEquals("\"name\" 10.0 1469751813 source=\"source\" \"foo\"=\"bar\" \"bar\"=\"foo\"",
        WavefrontDirectSender.pointToString("name",10L, 1469751813L, "source",
            ImmutableMap.of("foo", "bar", "bar", "foo")));
  }

  @Test
  public void testHistogramToString() {
    // null distribution
    assertNull(WavefrontDirectSender.histogramToString(HistogramGranularity.MINUTE, null, null, "name", "source", null));

    // empty distribution
    List<Pair<Double, Integer>> distribution = new ArrayList<>();
    assertNull(WavefrontDirectSender.histogramToString(HistogramGranularity.MINUTE, null, distribution, "name", "source", null));

    // distribution of size 1 where nullable arguments are null
    distribution.add(new Pair<>(1.23, 5));
    Assert.assertEquals("!M #5 1.23 \"name\" source=\"source\"",
        WavefrontDirectSender.histogramToString(HistogramGranularity.MINUTE, null, distribution, "name", "source", null));
    assertNull(WavefrontDirectSender.histogramToString(null, null, distribution, "name", "source", null));
    assertNull(WavefrontDirectSender.histogramToString(HistogramGranularity.MINUTE, null, distribution, null, "source", null));
    assertNull(WavefrontDirectSender.histogramToString(HistogramGranularity.MINUTE, null, distribution, "name", null, null));

    // distribution that contains null value
    distribution.add(new Pair<>(null, 5));
    assertNull(WavefrontDirectSender.histogramToString(HistogramGranularity.MINUTE, null, distribution, "name", "source", null));
    distribution.remove(distribution.size() - 1);

    // distribution that contains null count
    distribution.add(new Pair<>(1.23, null));
    assertNull(WavefrontDirectSender.histogramToString(HistogramGranularity.MINUTE, null, distribution, "name", "source", null));
    distribution.remove(distribution.size() - 1);

    // distribution that contains count <= 0
    distribution.add(new Pair<>(1.23, 0));
    assertNull(WavefrontDirectSender.histogramToString(HistogramGranularity.MINUTE, null, distribution, "name", "source", null));
    distribution.remove(distribution.size() - 1);

    // distribution of size > 1 where nullable arguments are not null
    distribution.add(new Pair<>(10.0, 1));
    distribution.add(new Pair<>(456346.3453453451, 501));
    distribution.add(new Pair<>(1.23, 1));
    distribution.add(new Pair<>(-9.3, 12));
    Assert.assertEquals("!H 1532456268 #5 1.23 #1 10.0 #501 456346.3453453451 #1 1.23 #12 -9.3 \"name\" source=\"source\" \"foo\"=\"bar\" \"bar\"=\"foo\"",
        WavefrontDirectSender.histogramToString(HistogramGranularity.HOUR, 1532456268L, distribution,
            "name", "source", ImmutableMap.of("foo", "bar", "bar", "foo")));
  }
}
