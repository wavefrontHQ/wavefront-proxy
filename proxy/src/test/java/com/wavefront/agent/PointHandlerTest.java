package com.wavefront.agent;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.time.DateUtils;

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
    Assert.assertTrue(PointHandlerImpl.charactersAreValid(input));

    input = "good.metric2";
    Assert.assertTrue(PointHandlerImpl.charactersAreValid(input));

    input = "good-metric3";
    Assert.assertTrue(PointHandlerImpl.charactersAreValid(input));

    input = "good_metric4";
    Assert.assertTrue(PointHandlerImpl.charactersAreValid(input));

    input = "good,metric5";
    Assert.assertTrue(PointHandlerImpl.charactersAreValid(input));

    input = "good/metric6";
    Assert.assertTrue(PointHandlerImpl.charactersAreValid(input));

    input = "~good.metric7";
    Assert.assertTrue(PointHandlerImpl.charactersAreValid(input));

    input = "abcdefghijklmnopqrstuvwxyz.0123456789,/_-ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    Assert.assertTrue(PointHandlerImpl.charactersAreValid(input));

    input = "abcdefghijklmnopqrstuvwxyz.0123456789,/_-ABCDEFGHIJKLMNOPQRSTUVWXYZ~";
    Assert.assertFalse(PointHandlerImpl.charactersAreValid(input));

    input = "as;df";
    Assert.assertFalse(PointHandlerImpl.charactersAreValid(input));

    input = "as:df";
    Assert.assertFalse(PointHandlerImpl.charactersAreValid(input));

    input = "as df";
    Assert.assertFalse(PointHandlerImpl.charactersAreValid(input));

    input = "as'df";
    Assert.assertFalse(PointHandlerImpl.charactersAreValid(input));
  }

  @Test
  public void testPointAnnotationKeyValidation() {
    Map<String, String> goodMap = new HashMap<String, String>();
    goodMap.put("key", "value");

    Map<String, String> badMap = new HashMap<String, String>();
    badMap.put("k:ey", "value");

    ReportPoint rp = new ReportPoint("some metric", System.currentTimeMillis(), 10L, "host", "table",
        goodMap);
    Assert.assertTrue(PointHandlerImpl.annotationKeysAreValid(rp));

    rp.setAnnotations(badMap);
    Assert.assertFalse(PointHandlerImpl.annotationKeysAreValid(rp));

  }

  @Test
  public void testPointInRangeCorrectForTimeRanges() throws NoSuchMethodException, InvocationTargetException,
      IllegalAccessException {

    long millisPerYear = 31536000000L;
    long millisPerDay = 86400000L;
    int port = 2878;
    String validationLevel = "NUMERIC_ONLY";
    int blockedPointsPerBatch = 0;
    String prefix = null;
    long discardPointsHours = 8760; // number of hours in a year
    PostPushDataTimedTask[] postPushDataTimedTasks = null;


    // not in range if over a year ago
    ReportPoint rp = new ReportPoint("some metric", System.currentTimeMillis() - millisPerYear, 10L, "host", "table",
        new HashMap<String, String>());
    PointHandlerImpl pthandler = new PointHandlerImpl(port, validationLevel, blockedPointsPerBatch, prefix,
            discardPointsHours, postPushDataTimedTasks);
    Assert.assertFalse(pthandler.pointInRange(rp));

    rp.setTimestamp(System.currentTimeMillis() - millisPerYear - 1);
    Assert.assertFalse(pthandler.pointInRange(rp));

    // in range if within a year ago
    rp.setTimestamp(System.currentTimeMillis() - (millisPerYear / 2));
    Assert.assertTrue(pthandler.pointInRange(rp));

    // in range for right now
    rp.setTimestamp(System.currentTimeMillis());
    Assert.assertTrue(pthandler.pointInRange(rp));

    // in range if within a day in the future
    rp.setTimestamp(System.currentTimeMillis() + millisPerDay - 1);
    Assert.assertTrue(pthandler.pointInRange(rp));

    // out of range for over a day in the future
    rp.setTimestamp(System.currentTimeMillis() + (millisPerDay * 2));
    Assert.assertFalse(pthandler.pointInRange(rp));

    //discard points older than 24 hours
    discardPointsHours = 24;
    PointHandlerImpl pthandler1 = new PointHandlerImpl(port, validationLevel, blockedPointsPerBatch, prefix,
            discardPointsHours, postPushDataTimedTasks);
    rp.setTimestamp(System.currentTimeMillis() - millisPerDay) ;
    Assert.assertFalse(pthandler1.pointInRange(rp));


  }

  // This is a slow implementation of pointToString that is known to work to specification.
  private static String referenceImpl(ReportPoint point) {
    String toReturn = String.format("\"%s\" %s %d source=\"%s\"",
        point.getMetric().replaceAll("\"", "\\\""),
        point.getValue(),
        point.getTimestamp() / 1000,
        point.getHost().replaceAll("\"", "\\\""));
    for (Map.Entry<String, String> entry : point.getAnnotations().entrySet()) {
      toReturn += String.format(" \"%s\"=\"%s\"",
          entry.getKey().replaceAll("\"", "\\\""),
          entry.getValue().replaceAll("\"", "\\\""));
    }
    return toReturn;
  }

  private void testReportPointToStringHelper(ReportPoint rp) {
    Assert.assertEquals(referenceImpl(rp), PointHandlerImpl.pointToString(rp));
  }

  @Test
  public void testReportPointToString() {
    // Vanilla point
    testReportPointToStringHelper(new ReportPoint("some metric", 1469751813000L, 10L, "host", "table",
        ImmutableMap.of("foo", "bar", "boo", "baz")));
    // No tags
    testReportPointToStringHelper(new ReportPoint("some metric", 1469751813000L, 10L, "host", "table",
        new HashMap<String, String>()));
    // Quote in metric name
    testReportPointToStringHelper(new ReportPoint("some\"metric", 1469751813000L, 10L, "host", "table",
        new HashMap<String, String>()));
    // Quote in tags
    testReportPointToStringHelper(new ReportPoint("some metric", 1469751813000L, 10L, "host", "table",
        ImmutableMap.of("foo\"", "\"bar", "bo\"o", "baz")));
  }
}
