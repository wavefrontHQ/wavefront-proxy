package com.wavefront.agent.preprocessor;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import com.google.common.io.Files;
import com.wavefront.ingester.GraphiteDecoder;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import wavefront.report.ReportPoint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PreprocessorRulesTest {

  private static final String FOO = "foo";
  private static final String SOURCE_NAME = "sourceName";
  private static final String METRIC_NAME = "metricName";
  private static PreprocessorConfigManager config;
  private final static List<String> emptyCustomSourceTags = Collections.emptyList();
  private final GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
  private final PreprocessorRuleMetrics metrics = new PreprocessorRuleMetrics(null, null, null);

  @BeforeClass
  public static void setup() throws IOException {
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules.yaml");
    config = new PreprocessorConfigManager();
    config.loadFromStream(stream);
  }

  @Test
  public void testPreprocessorRulesHotReload() throws Exception {
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    String path = File.createTempFile("proxyPreprocessorRulesFile", null).getPath();
    File file = new File(path);
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules.yaml");
    Files.asCharSink(file, Charsets.UTF_8).writeFrom(new InputStreamReader(stream));
    config.loadFile(path);
    ReportableEntityPreprocessor preprocessor = config.get("2878").get();
    assertEquals(1, preprocessor.forPointLine().getFilters().size());
    assertEquals(1, preprocessor.forPointLine().getTransformers().size());
    assertEquals(3, preprocessor.forReportPoint().getFilters().size());
    assertEquals(8, preprocessor.forReportPoint().getTransformers().size());
    config.loadFileIfModified(path); // should be no changes
    preprocessor = config.get("2878").get();
    assertEquals(1, preprocessor.forPointLine().getFilters().size());
    assertEquals(1, preprocessor.forPointLine().getTransformers().size());
    assertEquals(3, preprocessor.forReportPoint().getFilters().size());
    assertEquals(8, preprocessor.forReportPoint().getTransformers().size());
    stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_reload.yaml");
    Files.asCharSink(file, Charsets.UTF_8).writeFrom(new InputStreamReader(stream));
    // this is only needed for JDK8. JDK8 has second-level precision of lastModified,
    // in JDK11 lastModified is in millis.
    file.setLastModified((file.lastModified() / 1000 + 1) * 1000);
    config.loadFileIfModified(path); // reload should've happened
    preprocessor = config.get("2878").get();
    assertEquals(0, preprocessor.forPointLine().getFilters().size());
    assertEquals(2, preprocessor.forPointLine().getTransformers().size());
    assertEquals(1, preprocessor.forReportPoint().getFilters().size());
    assertEquals(3, preprocessor.forReportPoint().getTransformers().size());
    config.setUpConfigFileMonitoring(path, 1000);
  }

  @Test
  public void testPointInRangeCorrectForTimeRanges() {
    long millisPerYear = 31536000000L;
    long millisPerDay = 86400000L;
    long millisPerHour = 3600000L;

    long time = System.currentTimeMillis();
    AnnotatedPredicate<ReportPoint> pointInRange1year = new ReportPointTimestampInRangeFilter(8760,
        24, () -> time);
    // not in range if over a year ago
    ReportPoint rp = new ReportPoint("some metric", time - millisPerYear, 10L, "host", "table",
        new HashMap<>());
    Assert.assertFalse(pointInRange1year.test(rp));

    rp.setTimestamp(time - millisPerYear - 1);
    Assert.assertFalse(pointInRange1year.test(rp));

    // in range if within a year ago
    rp.setTimestamp(time - (millisPerYear / 2));
    Assert.assertTrue(pointInRange1year.test(rp));

    // in range for right now
    rp.setTimestamp(time);
    Assert.assertTrue(pointInRange1year.test(rp));

    // in range if within a day in the future
    rp.setTimestamp(time + millisPerDay - 1);
    Assert.assertTrue(pointInRange1year.test(rp));

    // out of range for over a day in the future
    rp.setTimestamp(time + (millisPerDay * 2));
    Assert.assertFalse(pointInRange1year.test(rp));

    // now test with 1 day limit
    AnnotatedPredicate<ReportPoint> pointInRange1day = new ReportPointTimestampInRangeFilter(24,
        24, () -> time);

    rp.setTimestamp(time - millisPerDay - 1);
    Assert.assertFalse(pointInRange1day.test(rp));

    // in range if within 1 day ago
    rp.setTimestamp(time - (millisPerDay / 2));
    Assert.assertTrue(pointInRange1day.test(rp));

    // in range for right now
    rp.setTimestamp(time);
    Assert.assertTrue(pointInRange1day.test(rp));

    // assert for future range within 12 hours
    AnnotatedPredicate<ReportPoint> pointInRange12hours = new ReportPointTimestampInRangeFilter(12,
        12, () -> time);

    rp.setTimestamp(time + (millisPerHour * 10));
    Assert.assertTrue(pointInRange12hours.test(rp));

    rp.setTimestamp(time - (millisPerHour * 10));
    Assert.assertTrue(pointInRange12hours.test(rp));

    rp.setTimestamp(time + (millisPerHour * 20));
    Assert.assertFalse(pointInRange12hours.test(rp));

    rp.setTimestamp(time - (millisPerHour * 20));
    Assert.assertFalse(pointInRange12hours.test(rp));

    AnnotatedPredicate<ReportPoint> pointInRange10Days = new ReportPointTimestampInRangeFilter(240,
        240, () -> time);

    rp.setTimestamp(time + (millisPerDay * 9));
    Assert.assertTrue(pointInRange10Days.test(rp));

    rp.setTimestamp(time - (millisPerDay * 9));
    Assert.assertTrue(pointInRange10Days.test(rp));

    rp.setTimestamp(time + (millisPerDay * 20));
    Assert.assertFalse(pointInRange10Days.test(rp));

    rp.setTimestamp(time - (millisPerDay * 20));
    Assert.assertFalse(pointInRange10Days.test(rp));
  }

  @Test(expected = NullPointerException.class)
  public void testLineReplaceRegexNullMatchThrows() {
    // try to create a regex replace rule with a null match pattern
    LineBasedReplaceRegexTransformer invalidRule = new LineBasedReplaceRegexTransformer(null, FOO, null, null, metrics);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLineReplaceRegexBlankMatchThrows() {
    // try to create a regex replace rule with a blank match pattern
    LineBasedReplaceRegexTransformer invalidRule = new LineBasedReplaceRegexTransformer("", FOO, null, null, metrics);
  }

  @Test(expected = NullPointerException.class)
  public void testLineAllowNullMatchThrows() {
    // try to create an allow rule with a null match pattern
    LineBasedAllowFilter invalidRule = new LineBasedAllowFilter(null, metrics);
  }

  @Test(expected = NullPointerException.class)
  public void testLineBlockNullMatchThrows() {
    // try to create a block rule with a null match pattern
    LineBasedBlockFilter invalidRule = new LineBasedBlockFilter(null, metrics);
  }

  @Test(expected = NullPointerException.class)
  public void testPointBlockNullScopeThrows() {
    // try to create a block rule with a null scope
    ReportPointBlockFilter invalidRule = new ReportPointBlockFilter(null, FOO,null, metrics);
  }

  @Test(expected = NullPointerException.class)
  public void testPointBlockNullMatchThrows() {
    // try to create a block rule with a null pattern
    ReportPointBlockFilter invalidRule = new ReportPointBlockFilter(FOO, null,
        null, metrics);
  }

  @Test(expected = NullPointerException.class)
  public void testPointAllowNullScopeThrows() {
    // try to create an allow rule with a null scope
    ReportPointAllowFilter invalidRule = new ReportPointAllowFilter(null, FOO, null, metrics);
  }

  @Test(expected = NullPointerException.class)
  public void testPointAllowNullMatchThrows() {
    // try to create a block rule with a null pattern
    ReportPointAllowFilter invalidRule = new ReportPointAllowFilter(FOO, null, null, metrics);
  }

  @Test
  public void testReportPointFiltersWithValidV2AndInvalidV1Predicate() {
    try {
      ReportPointAllowFilter invalidRule = new ReportPointAllowFilter("metricName",
          null, x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // Expected.
    }

    try {
      ReportPointAllowFilter invalidRule = new ReportPointAllowFilter(null,
          "^host$", x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // Expected.
    }

    try {
      ReportPointAllowFilter invalidRule = new ReportPointAllowFilter
          ("metricName", "^host$", x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // Expected.
    }

    try {
      ReportPointBlockFilter invalidRule = new ReportPointBlockFilter("metricName",
          null, x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // Expected.
    }

    try {
      ReportPointBlockFilter invalidRule = new ReportPointBlockFilter(null,
          "^host$", x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // Expected.
    }

    try {
      ReportPointBlockFilter invalidRule = new ReportPointBlockFilter
          ("metricName", "^host$", x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // Expected.
    }
  }

  @Test
  public void testReportPointFiltersWithValidV2AndV1Predicate() {
    ReportPointAllowFilter validAllowRule = new ReportPointAllowFilter
        (null, null, x -> false, metrics);

    ReportPointBlockFilter validBlocktRule = new ReportPointBlockFilter
        (null, null, x -> false, metrics);
  }

  @Test
  public void testPointLineRules() {
    String testPoint1 = "collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz";
    String testPoint2 = "collectd.#cpu#.&loadavg^.1m 7 1459527231 source=source$hostname foo=bar boo=baz";
    String testPoint3 = "collectd.cpu.loadavg.1m;foo=bar;boo=baz;tag=extra 7 1459527231 source=hostname";

    LineBasedReplaceRegexTransformer rule1 = new LineBasedReplaceRegexTransformer("(boo)=baz", "$1=qux", null, null, metrics);
    LineBasedReplaceRegexTransformer rule2 = new LineBasedReplaceRegexTransformer("[#&\\$\\^]", "", null, null, metrics);
    LineBasedBlockFilter rule3 = new LineBasedBlockFilter(".*source=source.*", metrics);
    LineBasedAllowFilter rule4 = new LineBasedAllowFilter(".*source=source.*", metrics);
    LineBasedReplaceRegexTransformer rule5 = new LineBasedReplaceRegexTransformer("cpu", "gpu", ".*hostname.*", null, metrics);
    LineBasedReplaceRegexTransformer rule6 = new LineBasedReplaceRegexTransformer("cpu", "gpu", ".*nomatch.*", null, metrics);
    LineBasedReplaceRegexTransformer rule7 = new LineBasedReplaceRegexTransformer("([^;]*);([^; ]*)([ ;].*)", "$1$3 $2", null, 2, metrics);

    String expectedPoint1 = "collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=qux";
    String expectedPoint2 = "collectd.cpu.loadavg.1m 7 1459527231 source=sourcehostname foo=bar boo=baz";
    String expectedPoint5 = "collectd.gpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz";
    String expectedPoint7 = "collectd.cpu.loadavg.1m;tag=extra 7 1459527231 source=hostname foo=bar boo=baz";

    assertEquals(expectedPoint1, rule1.apply(testPoint1));
    assertEquals(expectedPoint2, rule2.apply(testPoint2));
    assertTrue(rule3.test(testPoint1));
    assertFalse(rule3.test(testPoint2));
    assertFalse(rule4.test(testPoint1));
    assertTrue(rule4.test(testPoint2));
    assertEquals(expectedPoint5, rule5.apply(testPoint1));
    assertEquals(testPoint1, rule6.apply(testPoint1));
    assertEquals(expectedPoint7, rule7.apply(testPoint3));
  }

  @Test
  public void testReportPointRules() {
    String pointLine = "\"Some Metric\" 10.0 1469751813 source=\"Host\" \"boo\"=\"Baz\" \"foo\"=\"bar\"";
    ReportPoint point = parsePointLine(pointLine);

    // lowercase a point tag value with no match - shouldn't change anything
    new ReportPointForceLowercaseTransformer("boo", "nomatch.*", null, metrics).apply(point);
    assertEquals(pointLine, referencePointToStringImpl(point));

    // lowercase a point tag value - shouldn't affect metric name or source
    new ReportPointForceLowercaseTransformer("boo", null, null, metrics).apply(point);
    String expectedPoint1a = "\"Some Metric\" 10.0 1469751813 source=\"Host\" \"boo\"=\"baz\" \"foo\"=\"bar\"";
    assertEquals(expectedPoint1a, referencePointToStringImpl(point));

    // lowercase a metric name - shouldn't affect remaining source
    new ReportPointForceLowercaseTransformer(METRIC_NAME, null, null, metrics).apply(point);
    String expectedPoint1b = "\"some metric\" 10.0 1469751813 source=\"Host\" \"boo\"=\"baz\" \"foo\"=\"bar\"";
    assertEquals(expectedPoint1b, referencePointToStringImpl(point));

    // lowercase source
    new ReportPointForceLowercaseTransformer(SOURCE_NAME, null, null, metrics).apply(point);
    assertEquals(pointLine.toLowerCase(), referencePointToStringImpl(point));

    // try to remove a point tag when value doesn't match the regex - shouldn't change
    new ReportPointDropTagTransformer(FOO, "bar(never|match)", null, metrics).apply(point);
    assertEquals(pointLine.toLowerCase(), referencePointToStringImpl(point));

    // try to remove a point tag when value does match the regex - should work
    new ReportPointDropTagTransformer(FOO, "ba.", null, metrics).apply(point);
    String expectedPoint1 = "\"some metric\" 10.0 1469751813 source=\"host\" \"boo\"=\"baz\"";
    assertEquals(expectedPoint1, referencePointToStringImpl(point));

    // try to remove a point tag without a regex specified - should work
    new ReportPointDropTagTransformer("boo", null, null, metrics).apply(point);
    String expectedPoint2 = "\"some metric\" 10.0 1469751813 source=\"host\"";
    assertEquals(expectedPoint2, referencePointToStringImpl(point));

    // add a point tag back
    new ReportPointAddTagTransformer("boo", "baz", null, metrics).apply(point);
    String expectedPoint3 = "\"some metric\" 10.0 1469751813 source=\"host\" \"boo\"=\"baz\"";
    assertEquals(expectedPoint3, referencePointToStringImpl(point));

    // try to add a duplicate point tag - shouldn't change
    new ReportPointAddTagIfNotExistsTransformer("boo", "bar", null, metrics).apply(point);
    assertEquals(expectedPoint3, referencePointToStringImpl(point));

    // add another point tag back - should work this time
    new ReportPointAddTagIfNotExistsTransformer(FOO, "bar", null, metrics).apply(point);
    assertEquals(pointLine.toLowerCase(), referencePointToStringImpl(point));

    // rename a point tag - should work
    new ReportPointRenameTagTransformer(FOO, "qux", null, null, metrics).apply(point);
    String expectedPoint4 = "\"some metric\" 10.0 1469751813 source=\"host\" \"boo\"=\"baz\" \"qux\"=\"bar\"";
    assertEquals(expectedPoint4, referencePointToStringImpl(point));

    // rename a point tag matching the regex - should work
    new ReportPointRenameTagTransformer("boo", FOO, "b[a-z]z", null, metrics).apply(point);
    String expectedPoint5 = "\"some metric\" 10.0 1469751813 source=\"host\" \"foo\"=\"baz\" \"qux\"=\"bar\"";
    assertEquals(expectedPoint5, referencePointToStringImpl(point));

    // try to rename a point tag that doesn't match the regex - shouldn't change
    new ReportPointRenameTagTransformer(FOO, "boo", "wat", null, metrics).apply(point);
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
    new ReportPointReplaceRegexTransformer(METRIC_NAME, "Z", "", null, null, null, metrics).apply(point);
    assertEquals(expectedPoint6, referencePointToStringImpl(point));

    // replace regex in metric name - shouldn't affect anything else
    new ReportPointReplaceRegexTransformer(METRIC_NAME, "o", "0", null, null, null, metrics).apply(point);
    String expectedPoint7 = "\"prefix.s0me metric\" 10.0 1469751813 source=\"host\" \"foo\"=\"baz\" \"qux\"=\"bar\"";
    assertEquals(expectedPoint7, referencePointToStringImpl(point));

    // replace regex in source name - shouldn't affect anything else
    new ReportPointReplaceRegexTransformer(SOURCE_NAME, "o", "0", null, null, null, metrics).apply(point);
    String expectedPoint8 = "\"prefix.s0me metric\" 10.0 1469751813 source=\"h0st\" \"foo\"=\"baz\" \"qux\"=\"bar\"";
    assertEquals(expectedPoint8, referencePointToStringImpl(point));

    // replace regex in a point tag value - shouldn't affect anything else
    new ReportPointReplaceRegexTransformer(FOO, "b", "z", null, null, null, metrics).apply(point);
    String expectedPoint9 = "\"prefix.s0me metric\" 10.0 1469751813 source=\"h0st\" \"foo\"=\"zaz\" \"qux\"=\"bar\"";
    assertEquals(expectedPoint9, referencePointToStringImpl(point));

    // replace regex in a point tag value with matching groups
    new ReportPointReplaceRegexTransformer("qux", "([a-c][a-c]).", "$1z", null, null, null, metrics).apply(point);
    String expectedPoint10 = "\"prefix.s0me metric\" 10.0 1469751813 source=\"h0st\" \"foo\"=\"zaz\" \"qux\"=\"baz\"";
    assertEquals(expectedPoint10, referencePointToStringImpl(point));

    // replace regex in a point tag value with placeholders
    // try to substitute sourceName, a point tag value and a non-existent point tag
    new ReportPointReplaceRegexTransformer("qux", "az",
        "{{foo}}-{{no_match}}-g{{sourceName}}-{{metricName}}-{{}}", null, null, null, metrics).apply(point);
    String expectedPoint11 =
        "\"prefix.s0me metric\" 10.0 1469751813 source=\"h0st\" \"foo\"=\"zaz\" " +
            "\"qux\"=\"bzaz--gh0st-prefix.s0me metric-{{}}\"";
    assertEquals(expectedPoint11, referencePointToStringImpl(point));

  }

  @Test
  public void testAgentPreprocessorForPointLine() {

    // test point line transformers
    String testPoint1 = "collectd.#cpu#.&load$avg^.1m 7 1459527231 source=source$hostname foo=bar boo=baz";
    String expectedPoint1 = "collectd._cpu_._load_avg^.1m 7 1459527231 source=source_hostname foo=bar boo=baz";
    assertEquals(expectedPoint1, config.get("2878").get().forPointLine().transform(testPoint1));

    // test filters
    String testPoint2 = "collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz";
    assertTrue(config.get("2878").get().forPointLine().filter(testPoint2));

    String testPoint3 = "collectd.cpu.loadavg.1m 7 1459527231 source=hostname bar=foo boo=baz";
    assertFalse(config.get("2878").get().forPointLine().filter(testPoint3));
  }

  @Test
  public void testAgentPreprocessorForReportPoint() {
    ReportPoint testPoint1 = parsePointLine("collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz");
    assertTrue(config.get("2878").get().forReportPoint().filter(testPoint1));

    ReportPoint testPoint2 = parsePointLine("foo.collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz");
    assertFalse(config.get("2878").get().forReportPoint().filter(testPoint2));

    ReportPoint testPoint3 = parsePointLine("collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=west123 boo=baz");
    assertFalse(config.get("2878").get().forReportPoint().filter(testPoint3));

    ReportPoint testPoint4 = parsePointLine("collectd.cpu.loadavg.1m 7 1459527231 source=bar123 foo=bar boo=baz");
    assertFalse(config.get("2878").get().forReportPoint().filter(testPoint4));

    // in this test we are confirming that the rule sets for different ports are in fact different
    // on port 2878 we add "newtagkey=1", on port 4242 we don't
    ReportPoint testPoint1a = parsePointLine("collectd.cpu.loadavg.1m 7 1459527231 source=hostname foo=bar boo=baz");
    config.get("2878").get().forReportPoint().transform(testPoint1);
    config.get("4242").get().forReportPoint().transform(testPoint1a);
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

    // in this test the following should happen:
    // - fromMetric point tag extracted
    // - "node2" removed from the metric name
    // - fromSource point tag extracted
    // - fromTag point tag extracted
    String expectedPoint7 = "\"node0.node1.testExtractTag.node4\" 7.0 1459527231 source=\"host0-host1\" " +
        "\"fromMetric\"=\"node2\" \"fromSource\"=\"host2\" \"fromTag\"=\"tag0\" " +
        "\"testExtractTag\"=\"tag0.tag1.tag2.tag3.tag4\"";
    assertEquals(expectedPoint7, applyAllTransformers(
        "node0.node1.node2.testExtractTag.node4 7.0 1459527231 source=host0-host1-host2 " +
            "testExtractTag=tag0.tag1.tag2.tag3.tag4", "1234"));

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

  @Test(expected = IllegalArgumentException.class)
  public void testReportPointLimitRuleDropMetricNameThrows() {
    new ReportPointLimitLengthTransformer(METRIC_NAME, 10, LengthLimitActionType.DROP, null, null, metrics);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReportPointLimitRuleDropSourceNameThrows() {
    new ReportPointLimitLengthTransformer(SOURCE_NAME, 10, LengthLimitActionType.DROP, null, null, metrics);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReportPointLimitRuleTruncateWithEllipsisMaxLengthLessThan3Throws() {
    new ReportPointLimitLengthTransformer("tagK", 2, LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS, null, null, metrics);
  }

  @Test
  public void testReportPointLimitRule() {
    String pointLine = "metric.name.1234567 1 1459527231 source=source.name.test foo=bar bar=bar1234567890";
    ReportPointLimitLengthTransformer rule;
    ReportPoint point;

    // ** metric name
    // no regex, metric gets truncated
    rule = new ReportPointLimitLengthTransformer(METRIC_NAME, 6, LengthLimitActionType.TRUNCATE, null, null, metrics);
    point = rule.apply(parsePointLine(pointLine));
    assertEquals(6, point.getMetric().length());

    // metric name matches, gets truncated
    rule = new ReportPointLimitLengthTransformer(METRIC_NAME, 6, LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS,
        "^metric.*", null, metrics);
    point = rule.apply(parsePointLine(pointLine));
    assertEquals(6, point.getMetric().length());
    assertTrue(point.getMetric().endsWith("..."));

    // metric name does not match, no change
    rule = new ReportPointLimitLengthTransformer(METRIC_NAME, 6, LengthLimitActionType.TRUNCATE, "nope.*", null, metrics);
    point = rule.apply(parsePointLine(pointLine));
    assertEquals("metric.name.1234567", point.getMetric());

    // ** source name
    // no regex, source gets truncated
    rule = new ReportPointLimitLengthTransformer(SOURCE_NAME, 11, LengthLimitActionType.TRUNCATE, null, null, metrics);
    point = rule.apply(parsePointLine(pointLine));
    assertEquals(11, point.getHost().length());

    // source name matches, gets truncated
    rule = new ReportPointLimitLengthTransformer(SOURCE_NAME, 11, LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS,
        "^source.*", null, metrics);
    point = rule.apply(parsePointLine(pointLine));
    assertEquals(11, point.getHost().length());
    assertTrue(point.getHost().endsWith("..."));

    // source name does not match, no change
    rule = new ReportPointLimitLengthTransformer(SOURCE_NAME, 11, LengthLimitActionType.TRUNCATE, "nope.*", null, metrics);
    point = rule.apply(parsePointLine(pointLine));
    assertEquals("source.name.test", point.getHost());

    // ** tags
    // no regex, point tag gets truncated
    rule = new ReportPointLimitLengthTransformer("bar", 10, LengthLimitActionType.TRUNCATE, null, null, metrics);
    point = rule.apply(parsePointLine(pointLine));
    assertEquals(10, point.getAnnotations().get("bar").length());

    // point tag matches, gets truncated
    rule = new ReportPointLimitLengthTransformer("bar", 10, LengthLimitActionType.TRUNCATE, ".*456.*", null, metrics);
    point = rule.apply(parsePointLine(pointLine));
    assertEquals(10, point.getAnnotations().get("bar").length());

    // point tag does not match, no change
    rule = new ReportPointLimitLengthTransformer("bar", 10, LengthLimitActionType.TRUNCATE, ".*nope.*", null, metrics);
    point = rule.apply(parsePointLine(pointLine));
    assertEquals("bar1234567890", point.getAnnotations().get("bar"));

    // no regex, truncate with ellipsis
    rule = new ReportPointLimitLengthTransformer("bar", 10, LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS, null,
        null, metrics);
    point = rule.apply(parsePointLine(pointLine));
    assertEquals(10, point.getAnnotations().get("bar").length());
    assertTrue(point.getAnnotations().get("bar").endsWith("..."));

    // point tag matches, gets dropped
    rule = new ReportPointLimitLengthTransformer("bar", 10, LengthLimitActionType.DROP, ".*456.*", null, metrics);
    point = rule.apply(parsePointLine(pointLine));
    assertNull(point.getAnnotations().get("bar"));
  }

  @Test
  public void testPreprocessorUtil() {
    assertEquals("input...", PreprocessorUtil.truncate("inputInput", 8,
        LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS));
    assertEquals("inputI", PreprocessorUtil.truncate("inputInput", 6,
        LengthLimitActionType.TRUNCATE));
    try {
      PreprocessorUtil.truncate("input", 1, LengthLimitActionType.DROP);
      fail();
    } catch (IllegalArgumentException e) {
      // ok
    }
  }

  private boolean applyAllFilters(String pointLine, String strPort) {
    if (!config.get(strPort).get().forPointLine().filter(pointLine))
      return false;
    ReportPoint point = parsePointLine(pointLine);
    return config.get(strPort).get().forReportPoint().filter(point);
  }

  private String applyAllTransformers(String pointLine, String strPort) {
    String transformedPointLine = config.get(strPort).get().forPointLine().transform(pointLine);
    ReportPoint point = parsePointLine(transformedPointLine);
    config.get(strPort).get().forReportPoint().transform(point);
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
