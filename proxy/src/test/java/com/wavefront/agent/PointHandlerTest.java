package com.wavefront.agent;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import sunnylabs.report.ReportPoint;

/**
 * @author Andrew Kao (andrew@wavefront.com), Jason Bau (jbau@wavefront.com)
 */
public class PointHandlerTest {

  private static final Logger logger = LoggerFactory.getLogger(PointHandlerTest.class);

  @Test
  public void testPointIllegalChars() {
    String input = "metric1";
    Assert.assertTrue(PointHandler.charactersAreValid(input));

    input = "good.metric2";
    Assert.assertTrue(PointHandler.charactersAreValid(input));

    input = "good-metric3";
    Assert.assertTrue(PointHandler.charactersAreValid(input));

    input = "good_metric4";
    Assert.assertTrue(PointHandler.charactersAreValid(input));

    input = "good,metric5";
    Assert.assertTrue(PointHandler.charactersAreValid(input));

    input = "good/metric6";
    Assert.assertTrue(PointHandler.charactersAreValid(input));

    input = "~good.metric7";
    Assert.assertTrue(PointHandler.charactersAreValid(input));

    input = "abcdefghijklmnopqrstuvwxyz.0123456789,/_-ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    Assert.assertTrue(PointHandler.charactersAreValid(input));

    input = "abcdefghijklmnopqrstuvwxyz.0123456789,/_-ABCDEFGHIJKLMNOPQRSTUVWXYZ~";
    Assert.assertFalse(PointHandler.charactersAreValid(input));

    input = "as;df";
    Assert.assertFalse(PointHandler.charactersAreValid(input));

    input = "as:df";
    Assert.assertFalse(PointHandler.charactersAreValid(input));

    input = "as df";
    Assert.assertFalse(PointHandler.charactersAreValid(input));

    input = "as'df";
    Assert.assertFalse(PointHandler.charactersAreValid(input));
  }

  @Test
  public void testPointAnnotationKeyValidation() {
    Map<String, String> goodMap = new HashMap<String, String>();
    goodMap.put("key", "value");

    Map<String, String> badMap = new HashMap<String, String>();
    badMap.put("k:ey", "value");

    ReportPoint rp = new ReportPoint("some metric", System.currentTimeMillis(), 10L, "host",
        "table", goodMap);
    Assert.assertTrue(PointHandler.annotationKeysAreValid(rp));

    rp.setAnnotations(badMap);
    Assert.assertFalse(PointHandler.annotationKeysAreValid(rp));

  }

  @Test
  public void testPointInRangeCorrectForTimeRanges() throws NoSuchMethodException,
      InvocationTargetException, IllegalAccessException {

    long millisPerYear = 31536000000L;
    long millisPerDay = 86400000L;

    // not in range if over a year ago
    ReportPoint rp = new ReportPoint("some metric", System.currentTimeMillis() - millisPerYear,
        10L, "host", "table", new HashMap<String, String>());
    Assert.assertFalse(PointHandler.pointInRange(rp));

    rp.setTimestamp(System.currentTimeMillis() - millisPerYear - 1);
    Assert.assertFalse(PointHandler.pointInRange(rp));

    // in range if within a year ago
    rp.setTimestamp(System.currentTimeMillis() - (millisPerYear / 2));
    Assert.assertTrue(PointHandler.pointInRange(rp));

    // in range for right now
    rp.setTimestamp(System.currentTimeMillis());
    Assert.assertTrue(PointHandler.pointInRange(rp));

    // in range if within a day in the future
    rp.setTimestamp(System.currentTimeMillis() + millisPerDay - 1);
    Assert.assertTrue(PointHandler.pointInRange(rp));

    // out of range for over a day in the future
    rp.setTimestamp(System.currentTimeMillis() + (millisPerDay * 2));
    Assert.assertFalse(PointHandler.pointInRange(rp));
  }
}
