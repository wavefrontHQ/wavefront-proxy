package com.wavefront.agent.preprocessor;

import com.google.common.collect.Lists;

import com.wavefront.ingester.GraphiteDecoder;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;

import sunnylabs.report.ReportPoint;

public class PreprocessorRulesTest {

  private static AgentPreprocessorConfiguration config;
  private final static List<String> emptyCustomSourceTags = Collections.emptyList();
  private final GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);

  @BeforeClass
  public static void setup() throws IOException {
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules.yaml");
    config = new AgentPreprocessorConfiguration();
    config.loadFromStream(stream);
  }

  @Test
  public void testPointInRangeCorrectForTimeRanges() throws NoSuchMethodException, InvocationTargetException,
      IllegalAccessException {

    long millisPerYear = 31536000000L;
    long millisPerDay = 86400000L;

    AnnotatedPredicate<ReportPoint> pointInRange1year = new ReportPointTimestampInRangeFilter(8760);

    // not in range if over a year ago
    ReportPoint rp = new ReportPoint("some metric", System.currentTimeMillis() - millisPerYear, 10L, "host", "table",
        new HashMap<String, String>());
    Assert.assertFalse(pointInRange1year.apply(rp));

    rp.setTimestamp(System.currentTimeMillis() - millisPerYear - 1);
    Assert.assertFalse(pointInRange1year.apply(rp));

    // in range if within a year ago
    rp.setTimestamp(System.currentTimeMillis() - (millisPerYear / 2));
    Assert.assertTrue(pointInRange1year.apply(rp));

    // in range for right now
    rp.setTimestamp(System.currentTimeMillis());
    Assert.assertTrue(pointInRange1year.apply(rp));

    // in range if within a day in the future
    rp.setTimestamp(System.currentTimeMillis() + millisPerDay - 1);
    Assert.assertTrue(pointInRange1year.apply(rp));

    // out of range for over a day in the future
    rp.setTimestamp(System.currentTimeMillis() + (millisPerDay * 2));
    Assert.assertFalse(pointInRange1year.apply(rp));

    // now test with 1 day limit
    AnnotatedPredicate<ReportPoint> pointInRange1day = new ReportPointTimestampInRangeFilter(24);

    rp.setTimestamp(System.currentTimeMillis() - millisPerDay - 1);
    Assert.assertFalse(pointInRange1day.apply(rp));

    // in range if within 1 day ago
    rp.setTimestamp(System.currentTimeMillis() - (millisPerDay / 2));
    Assert.assertTrue(pointInRange1day.apply(rp));

    // in range for right now
    rp.setTimestamp(System.currentTimeMillis());
    Assert.assertTrue(pointInRange1day.apply(rp));
  }

  @Test(expected = NullPointerException.class)
  public void testLineReplaceRegexNullMatchThrows() {
    // try to create a regex replace rule with a null match pattern
    PointLineReplaceRegexTransformer invalidRule = new PointLineReplaceRegexTransformer(null, "foo", null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLineReplaceRegexBlankMatchThrows() {
    // try to create a regex replace rule with a blank match pattern
    PointLineReplaceRegexTransformer invalidRule = new PointLineReplaceRegexTransformer("", "foo", null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testLineWhitelistRegexNullMatchThrows() {
    // try to create a whitelist rule with a null match pattern
    PointLineWhitelistRegexFilter invalidRule = new PointLineWhitelistRegexFilter(null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testLineBlacklistRegexNullMatchThrows() {
    // try to create a blacklist rule with a null match pattern
    PointLineBlacklistRegexFilter invalidRule = new PointLineBlacklistRegexFilter(null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testPointBlacklistRegexNullScopeThrows() {
    // try to create a blacklist rule with a null scope
    ReportPointBlacklistRegexFilter invalidRule = new ReportPointBlacklistRegexFilter(null, "foo", null);
  }

  @Test(expected = NullPointerException.class)
  public void testPointBlacklistRegexNullMatchThrows() {
    // try to create a blacklist rule with a null pattern
    ReportPointBlacklistRegexFilter invalidRule = new ReportPointBlacklistRegexFilter("foo", null, null);
  }

  @Test(expected = NullPointerException.class)
  public void testPointWhitelistRegexNullScopeThrows() {
    // try to create a whitelist rule with a null scope
    ReportPointWhitelistRegexFilter invalidRule = new ReportPointWhitelistRegexFilter(null, "foo", null);
  }

  @Test(expected = NullPointerException.class)
  public void testPointWhitelistRegexNullMatchThrows() {
    // try to create a blacklist rule with a null pattern
    ReportPointWhitelistRegexFilter invalidRule = new ReportPointWhitelistRegexFilter("foo", null, null);
  }

  @Test
  public void testPointLineRules() {
    String testPoint1 = "collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz";
    String testPoint2 = "collectd.#cpu#.&loadavg^.1m 7 1459527231 source=source$hostname foo=bar boo=baz";

    PointLineReplaceRegexTransformer rule1 = new PointLineReplaceRegexTransformer("(boo)=baz", "$1=qux", null, null);
    PointLineReplaceRegexTransformer rule2 = new PointLineReplaceRegexTransformer("[#&\\$\\^]", "", null, null);
    PointLineBlacklistRegexFilter rule3 = new PointLineBlacklistRegexFilter(".*source=source.*", null);
    PointLineWhitelistRegexFilter rule4 = new PointLineWhitelistRegexFilter(".*source=source.*", null);
    PointLineReplaceRegexTransformer rule5 = new PointLineReplaceRegexTransformer("cpu", "gpu", ".*hostname.*", null);
    PointLineReplaceRegexTransformer rule6 = new PointLineReplaceRegexTransformer("cpu", "gpu", ".*nomatch.*", null);

    String expectedPoint1 = "collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=qux";
    String expectedPoint2 = "collectd.cpu.loadavg.1m 7 1459527231 source=sourcehostname foo=bar boo=baz";
    String expectedPoint5 = "collectd.gpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz";

    assertEquals(expectedPoint1, rule1.apply(testPoint1));
    assertEquals(expectedPoint2, rule2.apply(testPoint2));
    assertTrue(rule3.apply(testPoint1));
    assertFalse(rule3.apply(testPoint2));
    assertFalse(rule4.apply(testPoint1));
    assertTrue(rule4.apply(testPoint2));
    assertEquals(expectedPoint5, rule5.apply(testPoint1));
    assertEquals(testPoint1, rule6.apply(testPoint1));
  }

  @Test
  public void testReportPointRules() {
    String pointLine = "\"some metric\" 10.0 1469751813 source=\"host\" \"boo\"=\"baz\" \"foo\"=\"bar\"";
    ReportPoint point = parsePointLine(pointLine);

    // try to remove a point tag when value doesn't match the regex - shouldn't change
    new ReportPointDropTagTransformer("foo", "bar(never|match)", null).apply(point);
    assertEquals(pointLine, referencePointToStringImpl(point));

    // try to remove a point tag when value does match the regex - should work
    new ReportPointDropTagTransformer("foo", "ba.", null).apply(point);
    String expectedPoint1 = "\"some metric\" 10.0 1469751813 source=\"host\" \"boo\"=\"baz\"";
    assertEquals(expectedPoint1, referencePointToStringImpl(point));

    // try to remove a point tag without a regex specified - should work
    new ReportPointDropTagTransformer("boo", null, null).apply(point);
    String expectedPoint2 = "\"some metric\" 10.0 1469751813 source=\"host\"";
    assertEquals(expectedPoint2, referencePointToStringImpl(point));

    // add a point tag back
    new ReportPointAddTagTransformer("boo", "baz", null).apply(point);
    String expectedPoint3 = "\"some metric\" 10.0 1469751813 source=\"host\" \"boo\"=\"baz\"";
    assertEquals(expectedPoint3, referencePointToStringImpl(point));

    // try to add a duplicate point tag - shouldn't change
    new ReportPointAddTagIfNotExistsTransformer("boo", "bar", null).apply(point);
    assertEquals(expectedPoint3, referencePointToStringImpl(point));

    // add another point tag back - should work this time
    new ReportPointAddTagIfNotExistsTransformer("foo", "bar", null).apply(point);
    assertEquals(pointLine, referencePointToStringImpl(point));

    // rename a point tag - should work
    new ReportPointRenameTagTransformer("foo", "qux", null, null).apply(point);
    String expectedPoint4 = "\"some metric\" 10.0 1469751813 source=\"host\" \"boo\"=\"baz\" \"qux\"=\"bar\"";
    assertEquals(expectedPoint4, referencePointToStringImpl(point));

    // rename a point tag matching the regex - should work
    new ReportPointRenameTagTransformer("boo", "foo", "b[a-z]z", null).apply(point);
    String expectedPoint5 = "\"some metric\" 10.0 1469751813 source=\"host\" \"foo\"=\"baz\" \"qux\"=\"bar\"";
    assertEquals(expectedPoint5, referencePointToStringImpl(point));

    // try to rename a point tag that doesn't match the regex - shouldn't change
    new ReportPointRenameTagTransformer("foo", "boo", "wat", null).apply(point);
    assertEquals(expectedPoint5, referencePointToStringImpl(point));

    // add null metrics prefix - shouldn't change
    new ReportPointAddPrefixTransformer(null).apply(point);
    assertEquals(expectedPoint5, referencePointToStringImpl(point));

    // add blank metrics prefix - shouldn't change
    new ReportPointAddPrefixTransformer("").apply(point);
    assertEquals(expectedPoint5, referencePointToStringImpl(point));

    // add metrics prefix - should work
    new ReportPointAddPrefixTransformer("prefix").apply(point);
    String expectedPoint6 = "\"prefix.some metric\" 10.0 1469751813 source=\"host\" \"foo\"=\"baz\" \"qux\"=\"bar\"";
    assertEquals(expectedPoint6, referencePointToStringImpl(point));

    // replace regex in metric name, no matches - shouldn't change
    new ReportPointReplaceRegexTransformer("metricName", "Z", "", null, null).apply(point);
    assertEquals(expectedPoint6, referencePointToStringImpl(point));

    // replace regex in metric name - shouldn't affect anything else
    new ReportPointReplaceRegexTransformer("metricName", "o", "0", null, null).apply(point);
    String expectedPoint7 = "\"prefix.s0me metric\" 10.0 1469751813 source=\"host\" \"foo\"=\"baz\" \"qux\"=\"bar\"";
    assertEquals(expectedPoint7, referencePointToStringImpl(point));

    // replace regex in source name - shouldn't affect anything else
    new ReportPointReplaceRegexTransformer("sourceName", "o", "0", null, null).apply(point);
    String expectedPoint8 = "\"prefix.s0me metric\" 10.0 1469751813 source=\"h0st\" \"foo\"=\"baz\" \"qux\"=\"bar\"";
    assertEquals(expectedPoint8, referencePointToStringImpl(point));

    // replace regex in a point tag value - shouldn't affect anything else
    new ReportPointReplaceRegexTransformer("foo", "b", "z", null, null).apply(point);
    String expectedPoint9 = "\"prefix.s0me metric\" 10.0 1469751813 source=\"h0st\" \"foo\"=\"zaz\" \"qux\"=\"bar\"";
    assertEquals(expectedPoint9, referencePointToStringImpl(point));

    // replace regex in a point tag value with matching groups
    new ReportPointReplaceRegexTransformer("qux", "([a-c][a-c]).", "$1z", null, null).apply(point);
    String expectedPoint10 = "\"prefix.s0me metric\" 10.0 1469751813 source=\"h0st\" \"foo\"=\"zaz\" \"qux\"=\"baz\"";
    assertEquals(expectedPoint10, referencePointToStringImpl(point));
  }

  @Test
  public void testAgentPreprocessorForPointLine() {

    // test point line transformers
    String testPoint1 = "collectd.#cpu#.&load$avg^.1m 7 1459527231 source=source$hostname foo=bar boo=baz";
    String expectedPoint1 = "collectd._cpu_._load_avg^.1m 7 1459527231 source=source_hostname foo=bar boo=baz";
    assertEquals(expectedPoint1, config.forPort("2878").forPointLine().transform(testPoint1));

    // test filters
    String testPoint2 = "collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz";
    assertTrue(config.forPort("2878").forPointLine().filter(testPoint2));

    String testPoint3 = "collectd.cpu.loadavg.1m 7 1459527231 source=hostname bar=foo boo=baz";
    assertFalse(config.forPort("2878").forPointLine().filter(testPoint3));
  }

  @Test
  public void testAgentPreprocessorForReportPoint() {
    ReportPoint testPoint1 = parsePointLine("collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz");
    assertTrue(config.forPort("2878").forReportPoint().filter(testPoint1));

    ReportPoint testPoint2 = parsePointLine("foo.collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz");
    assertFalse(config.forPort("2878").forReportPoint().filter(testPoint2));

    ReportPoint testPoint3 = parsePointLine("collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=west123 boo=baz");
    assertFalse(config.forPort("2878").forReportPoint().filter(testPoint3));

    ReportPoint testPoint4 = parsePointLine("collectd.cpu.loadavg.1m 7 1459527231 source=bar123 foo=bar boo=baz");
    assertFalse(config.forPort("2878").forReportPoint().filter(testPoint4));

    // in this test we are confirming that the rule sets for different ports are in fact different
    // on port 2878 we add "newtagkey=1", on port 4242 we don't
    ReportPoint testPoint1a = parsePointLine("collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz");
    config.forPort("2878").forReportPoint().transform(testPoint1);
    config.forPort("4242").forReportPoint().transform(testPoint1a);
    String expectedPoint1 = "\"collectd.cpu.loadavg.1m\" 7.0 1459527231 " +
        "source=\"hostname\" \"baz\"=\"bar\" \"boo\"=\"baz\" \"newtagkey\"=\"1\"";
    String expectedPoint1a = "\"collectd.cpu.loadavg.1m\" 7.0 1459527231 " +
        "source=\"hostname\" \"baz\"=\"bar\" \"boo\"=\"baz\"";
    assertEquals(expectedPoint1, referencePointToStringImpl(testPoint1));
    assertEquals(expectedPoint1a, referencePointToStringImpl(testPoint1a));

    // in this test the following should happen:
    // - rename foo tag to baz
    // - "metrictest." prefix gets dropped from the metric name
    // - replace dashes with dots in bar tag
    String expectedPoint5 = "\"metric\" 7.0 1459527231 source=\"src\" " +
        "\"bar\"=\"baz.baz.baz\" \"baz\"=\"bar\" \"datacenter\"=\"az1\" \"newtagkey\"=\"1\" \"qux\"=\"123z\"";
    assertEquals(expectedPoint5, applyAllTransformers(
        "metrictest.metric 7 1459527231 source=src foo=bar datacenter=az1 bar=baz-baz-baz qux=123z", "2878"));

    // in this test the following should happen:
    // - rename tag foo to baz
    // - add new tag newtagkey=1
    // - drop dc1 tag
    // - drop datacenter tag as it matches az[4-6]
    // - rename qux tag to numericTag
    String expectedPoint6 = "\"some.metric\" 7.0 1459527231 source=\"hostname\" " +
        "\"baz\"=\"bar\" \"newtagkey\"=\"1\" \"numericTag\"=\"12345\" \"prefix\"=\"some\"";
    assertEquals(expectedPoint6, applyAllTransformers(
        "some.metric 7 1459527231 source=hostname foo=bar dc1=baz datacenter=az4 qux=12345", "2878"));
  }

  @Test
  public void testAllFilters() {
    assertTrue(applyAllFilters("valid.metric.loadavg.1m 7 1459527231 source=h.prod.corp foo=bar boo=baz", "1111"));
    assertTrue(applyAllFilters("valid.metric.loadavg.1m 7 1459527231 source=h.prod.corp foo=b_r boo=baz", "1111"));
    assertTrue(applyAllFilters("valid.metric.loadavg.1m 7 1459527231 source=h.prod.corp foo=b_r boo=baz", "1111"));
    assertFalse(applyAllFilters("invalid.metric.loadavg.1m 7 1459527231 source=h.prod.corp foo=bar boo=baz", "1111"));
    assertFalse(applyAllFilters("valid.metric.loadavg.1m 7 1459527231 source=h.prod.corp foo=bar baz=boo", "1111"));
    assertFalse(applyAllFilters("valid.metric.loadavg.1m 7 1459527231 source=h.dev.corp foo=bar boo=baz", "1111"));
    assertFalse(applyAllFilters("valid.metric.loadavg.1m 7 1459527231 source=h.prod.corp foo=bar boo=stop", "1111"));
    assertFalse(applyAllFilters("loadavg.1m 7 1459527231 source=h.prod.corp foo=bar boo=baz", "1111"));
  }

  private boolean applyAllFilters(String pointLine, String strPort) {
    if (!config.forPort(strPort).forPointLine().filter(pointLine))
      return false;
    ReportPoint point = parsePointLine(pointLine);
    return config.forPort(strPort).forReportPoint().filter(point);
  }

  private String applyAllTransformers(String pointLine, String strPort) {
    String transformedPointLine = config.forPort(strPort).forPointLine().transform(pointLine);
    ReportPoint point = parsePointLine(transformedPointLine);
    config.forPort(strPort).forReportPoint().transform(point);
    return referencePointToStringImpl(point);
  }

  private static String referencePointToStringImpl(ReportPoint point) {
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

  private ReportPoint parsePointLine(String pointLine) {
    List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
    decoder.decodeReportPoints(pointLine, points, "dummy");
    ReportPoint point = points.get(0);
    // convert annotations to TreeMap so the result is deterministic
    point.setAnnotations(new TreeMap<>(point.getAnnotations()));
    return point;
  }
}
