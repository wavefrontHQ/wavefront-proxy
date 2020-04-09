package com.wavefront.agent.preprocessor;

import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.wavefront.agent.preprocessor.PreprocessorUtil.LOGICAL_OPS;
import static com.wavefront.agent.preprocessor.PreprocessorUtil.V2_PREDICATE_KEY;

public class PreprocessorRuleV2PredicateTest {

  private static final String[] COMPARISON_OPS = {"equals", "startsWith", "contains", "endsWith",
      "regexMatch"};
  @Test
  public void testReportPointPreprocessorComparisonOps() {
    // test that preprocessor rules kick in before user rules
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_predicates.yaml");
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    Yaml yaml = new Yaml();
    Map<String, Object> rulesByPort = (Map<String, Object>) yaml.load(stream);
    ReportPoint point = null;
    for (String comparisonOp : Arrays.asList(COMPARISON_OPS)) {
      List<Map<String, Object>> rules = (List<Map<String, Object>>) rulesByPort.get
          (comparisonOp + "-reportpoint");
      Assert.assertEquals("Expected rule size :: ", 1, rules.size());
      Map<String, Object> v2PredicateMap = (Map<String, Object>) rules.get(0).get(V2_PREDICATE_KEY);
      Predicate v2Predicate = PreprocessorUtil.parsePredicate(v2PredicateMap, ReportPoint.class);
      Map<String, String> pointTags = new HashMap<>();
      switch (comparisonOp) {
        case "equals":
          point = new ReportPoint("foometric.1", System.currentTimeMillis(), 10L,
              "host", "table", null);
          Assert.assertTrue("Expected [equals-reportpoint] rule to return :: true , Actual :: " +
                  "false",
              v2Predicate.test(point));
          break;
        case "startsWith":
          point = new ReportPoint("foometric.2", System.currentTimeMillis(), 10L,
              "host", "table", null);
          Assert.assertTrue("Expected [startsWith-reportpoint] rule to return :: true , " +
                  "Actual :: false",
              v2Predicate.test(point));
          break;
        case "endsWith":
          point = new ReportPoint("foometric.3", System.currentTimeMillis(), 10L,
              "host-prod", "table", pointTags);
          Assert.assertTrue("Expected [endsWith-reportpoint] rule to return :: true , Actual :: " +
              "false", v2Predicate.test(point));
          break;
        case "regexMatch":
          point = new ReportPoint("foometric.4", System.currentTimeMillis(), 10L,
              "host", "table", null);
          Assert.assertTrue("Expected [regexMatch-reportpoint] rule to return :: true , Actual ::" +
                  " false", v2Predicate.test(point));
          break;
        case "contains":
          point = new ReportPoint("foometric.prod.test", System.currentTimeMillis(), 10L,
              "host-prod-test", "table", pointTags);
          Assert.assertTrue("Expected [contains-reportpoint] rule to return :: true , Actual :: " +
                  "false", v2Predicate.test(point));
          break;
        case "in":
          pointTags = new HashMap<>();
          pointTags.put("key1", "val3");
          point = new ReportPoint("foometric.4", System.currentTimeMillis(), 10L,
              "host", "table", pointTags);
          Assert.assertTrue("Expected [in-reportpoint] rule to return :: true , Actual :: " +
              "false", v2Predicate.test(point));
          break;
      }
    }
  }

  @Test
  public void testReportPointPreprocessorComparisonOpsList() {
    // test that preprocessor rules kick in before user rules
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_predicates.yaml");
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    Yaml yaml = new Yaml();
    Map<String, Object> rulesByPort = (Map<String, Object>) yaml.load(stream);
    ReportPoint point = null;
    for (String comparisonOp : Arrays.asList(COMPARISON_OPS)) {
      List<Map<String, Object>> rules = (List<Map<String, Object>>) rulesByPort.get
          (comparisonOp + "-list-reportpoint");
      Assert.assertEquals("Expected rule size :: ", 1, rules.size());
      Map<String, Object> v2PredicateMap = (Map<String, Object>) rules.get(0).get(V2_PREDICATE_KEY);
      Predicate v2Predicate = PreprocessorUtil.parsePredicate(v2PredicateMap, ReportPoint.class);
      Map<String, String> pointTags = new HashMap<>();
      switch (comparisonOp) {
        case "equals":
          point = new ReportPoint("foometric.1", System.currentTimeMillis(), 10L,
              "host", "table", null);
          Assert.assertTrue("Expected [equals-reportpoint] rule to return :: true , Actual :: " +
                  "false",
              v2Predicate.test(point));
          break;
        case "startsWith":
          point = new ReportPoint("foometric.2", System.currentTimeMillis(), 10L,
              "host", "table", null);
          Assert.assertTrue("Expected [startsWith-reportpoint] rule to return :: true , " +
                  "Actual :: false",
              v2Predicate.test(point));
          break;
        case "endsWith":
          point = new ReportPoint("foometric.3", System.currentTimeMillis(), 10L,
              "host-prod", "table", pointTags);
          Assert.assertTrue("Expected [endsWith-reportpoint] rule to return :: true , Actual :: " +
              "false", v2Predicate.test(point));
          break;
        case "regexMatch":
          point = new ReportPoint("foometric.4", System.currentTimeMillis(), 10L,
              "host", "table", null);
          Assert.assertTrue("Expected [regexMatch-reportpoint] rule to return :: true , Actual ::" +
              " false", v2Predicate.test(point));
          break;
        case "contains":
          point = new ReportPoint("foometric.prod.test", System.currentTimeMillis(), 10L,
              "host-prod-test", "table", pointTags);
          Assert.assertTrue("Expected [contains-reportpoint] rule to return :: true , Actual :: " +
              "false", v2Predicate.test(point));
          break;
        case "in":
          pointTags = new HashMap<>();
          pointTags.put("key1", "val3");
          point = new ReportPoint("foometric.4", System.currentTimeMillis(), 10L,
              "host", "table", pointTags);
          Assert.assertTrue("Expected [in-reportpoint] rule to return :: true , Actual :: " +
              "false", v2Predicate.test(point));
          break;
      }
    }
  }

  @Test
  public void testSpanPreprocessorComparisonOpsList() {
    // test that preprocessor rules kick in before user rules
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_predicates.yaml");
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    Yaml yaml = new Yaml();
    Map<String, Object> rulesByPort = (Map<String, Object>) yaml.load(stream);
    String spanLine = "testSpanName source=spanSourceName spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 foo=BAR1-1234567890 foo=BAR2-2345678901 foo=bAr2-3456789012 " +
        "foo=baR boo=baz 1532012145123 1532012146234";
    Span span = null;
    for (String comparisonOp : Arrays.asList(COMPARISON_OPS)) {
      List<Map<String, Object>> rules = (List<Map<String, Object>>) rulesByPort.get
          (comparisonOp + "-list-span");
      Assert.assertEquals("Expected rule size :: ", 1, rules.size());
      Map<String, Object> v2PredicateMap = (Map<String, Object>) rules.get(0).get(V2_PREDICATE_KEY);
      Predicate v2Predicate = PreprocessorUtil.parsePredicate(v2PredicateMap, Span.class);
      Map<String, String> pointTags = new HashMap<>();
      switch (comparisonOp) {
        case "equals":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
              "source=spanSourceName " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 1532012145123 1532012146234");
          Assert.assertTrue("Expected [equals-span] rule to return :: true , Actual :: " +
                  "false",
              v2Predicate.test(span));
          break;
        case "startsWith":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.2 " +
              "source=spanSourceName " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 1532012145123 1532012146234");
          Assert.assertTrue("Expected [startsWith-span] rule to return :: true , " +
                  "Actual :: false",
              v2Predicate.test(span));
          break;
        case "endsWith":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
              "source=spanSourceName-prod " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 1532012145123 1532012146234");
          Assert.assertTrue("Expected [endsWith-span] rule to return :: true , Actual :: " +
              "false", v2Predicate.test(span));
          break;
        case "regexMatch":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
              "source=host " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 1532012145123 1532012146234");
          Assert.assertTrue("Expected [regexMatch-span] rule to return :: true , Actual ::" +
              " false", v2Predicate.test(span));
          break;
        case "contains":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
              "source=spanSourceName-prod-3 " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 1532012145123 1532012146234");
          Assert.assertTrue("Expected [contains-span] rule to return :: true , Actual :: " +
              "false", v2Predicate.test(span));
          break;
        case "in":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
              "source=spanSourceName-prod " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 " +
              "key1=val2 1532012145123 1532012146234");
          Assert.assertTrue("Expected [in-span] rule to return :: true , Actual :: " +
              "false", v2Predicate.test(span));
          break;
      }
    }
  }

  @Test
  public void testSpanPreprocessorComparisonOps() {
    // test that preprocessor rules kick in before user rules
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_predicates.yaml");
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    Yaml yaml = new Yaml();
    Map<String, Object> rulesByPort = (Map<String, Object>) yaml.load(stream);
    String spanLine = "testSpanName source=spanSourceName spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 foo=BAR1-1234567890 foo=BAR2-2345678901 foo=bAr2-3456789012 " +
        "foo=baR boo=baz 1532012145123 1532012146234";
    Span span = null;
    for (String comparisonOp : Arrays.asList(COMPARISON_OPS)) {
      List<Map<String, Object>> rules = (List<Map<String, Object>>) rulesByPort.get
          (comparisonOp + "-span");
      Assert.assertEquals("Expected rule size :: ", 1, rules.size());
      Map<String, Object> v2PredicateMap = (Map<String, Object>) rules.get(0).get(V2_PREDICATE_KEY);
      Predicate v2Predicate = PreprocessorUtil.parsePredicate(v2PredicateMap, Span.class);
      Map<String, String> pointTags = new HashMap<>();
      switch (comparisonOp) {
        case "equals":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
              "source=spanSourceName " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 1532012145123 1532012146234");
          Assert.assertTrue("Expected [equals-span] rule to return :: true , Actual :: " +
                  "false",
              v2Predicate.test(span));
          break;
        case "startsWith":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.2 " +
              "source=spanSourceName " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 1532012145123 1532012146234");
          Assert.assertTrue("Expected [startsWith-span] rule to return :: true , " +
                  "Actual :: false",
              v2Predicate.test(span));
          break;
        case "endsWith":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
              "source=spanSourceName-prod " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 1532012145123 1532012146234");
          Assert.assertTrue("Expected [endsWith-span] rule to return :: true , Actual :: " +
              "false", v2Predicate.test(span));
          break;
        case "regexMatch":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
              "source=host " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 1532012145123 1532012146234");
          Assert.assertTrue("Expected [regexMatch-span] rule to return :: true , Actual ::" +
              " false", v2Predicate.test(span));
          break;
        case "contains":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
              "source=spanSourceName-prod-3 " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 1532012145123 1532012146234");
          Assert.assertTrue("Expected [contains-span] rule to return :: true , Actual :: " +
              "false", v2Predicate.test(span));
          break;
        case "in":
          span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
              "source=spanSourceName-prod " +
              "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
              "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 " +
              "key1=val2 1532012145123 1532012146234");
          Assert.assertTrue("Expected [in-span] rule to return :: true , Actual :: " +
              "false", v2Predicate.test(span));
          break;
      }
    }
  }

  @Test
  public void testPreprocessorReportPointLogicalOps() {
    // test that preprocessor rules kick in before user rules
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_predicates.yaml");
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    Yaml yaml = new Yaml();
    Map<String, Object> rulesByPort = (Map<String, Object>) yaml.load(stream);
    List<String> comparisonOpList = Arrays.asList(LOGICAL_OPS);
    List<Map<String, Object>> rules = (List<Map<String, Object>>) rulesByPort.get("logicalop-reportpoint");
    Assert.assertEquals("Expected rule size :: ", 1, rules.size());
    Map<String, Object> v2PredicateMap = (Map<String, Object>) rules.get(0).get(V2_PREDICATE_KEY);
    Predicate v2Predicate = PreprocessorUtil.parsePredicate(v2PredicateMap, ReportPoint.class);
    Map<String, String> pointTags = new HashMap<>();
    ReportPoint point = null;

    // Satisfies all requirements.
    pointTags.put("key1", "val1");
    pointTags.put("key2", "val2");
    point = new ReportPoint("foometric.1", System.currentTimeMillis(), 10L,
        "host", "table", pointTags);
    Assert.assertTrue("Expected [logicalop-reportpoint] rule to return :: true , Actual :: false",
        v2Predicate.test(point));

    // Tests for "ignore" : by not satisfying "regexMatch"/"equals" comparison
    pointTags = new HashMap<>();
    pointTags.put("key2", "val2");
    point = new ReportPoint("foometric.1", System.currentTimeMillis(), 10L,
        "host", "table", pointTags);
    Assert.assertTrue("Expected [logicalop-reportpoint] rule to return :: true , Actual :: " +
        "false", v2Predicate.test(point));

    // Tests for "all" : by not satisfying "equals" comparison
    pointTags = new HashMap<>();
    pointTags.put("key2", "val");
    point = new ReportPoint("foometric.1", System.currentTimeMillis(), 10L,
        "host", "table", pointTags);
    Assert.assertFalse("Expected [logicalop-reportpoint] rule to return :: false , Actual :: " +
        "true", v2Predicate.test(point));

    // Tests for "any" : by not satisfying "startsWith"/"endsWith" comparison
    pointTags = new HashMap<>();
    pointTags.put("key2", "val2");
    point = new ReportPoint("boometric.1", System.currentTimeMillis(), 10L,
        "host", "table", pointTags);
    Assert.assertFalse("Expected [logicalop-reportpoint] rule to return :: false , Actual :: " +
        "true", v2Predicate.test(point));

    // Tests for "none" : by satisfying "contains" comparison
    pointTags = new HashMap<>();
    pointTags.put("key2", "val2");
    pointTags.put("debug", "debug-istrue");
    point = new ReportPoint("foometric.1", System.currentTimeMillis(), 10L,
        "host", "table", pointTags);
    Assert.assertFalse("Expected [logicalop-reportpoint] rule to return :: false , Actual :: " +
        "true", v2Predicate.test(point));
  }

  @Test
  public void testPreprocessorSpanLogicalOps() {
    // test that preprocessor rules kick in before user rules
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_predicates.yaml");
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    Yaml yaml = new Yaml();
    Map<String, Object> rulesByPort = (Map<String, Object>) yaml.load(stream);
    List<String> comparisonOpList = Arrays.asList(LOGICAL_OPS);
    List<Map<String, Object>> rules = (List<Map<String, Object>>) rulesByPort.get("logicalop-span");
    Assert.assertEquals("Expected rule size :: ", 1, rules.size());
    Map<String, Object> v2PredicateMap = (Map<String, Object>) rules.get(0).get(V2_PREDICATE_KEY);
    Predicate v2Predicate = PreprocessorUtil.parsePredicate(v2PredicateMap, Span.class);
    Span span = null;

    // Satisfies all requirements.
    span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
        "source=spanSourceName-prod " +
        "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 " +
        "key1=val1 key2=val2 1532012145123 1532012146234");
    Assert.assertTrue("Expected [logicalop-reportpoint] rule to return :: true , Actual :: false",
        v2Predicate.test(span));

    // Tests for "ignore" : by not satisfying "regexMatch"/"equals" comparison
    span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
        "source=spanSourceName-prod " +
        "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 " +
        "key2=val2 1532012145123 1532012146234");
    Assert.assertTrue("Expected [logicalop-reportpoint] rule to return :: true , Actual :: " +
        "false", v2Predicate.test(span));

    // Tests for "all" : by not satisfying "equals" comparison
    span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
        "source=spanSourceName-prod " +
        "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 " +
        "key2=val 1532012145123 1532012146234");
    Assert.assertFalse("Expected [logicalop-reportpoint] rule to return :: false , Actual :: " +
        "true", v2Predicate.test(span));

    // Tests for "any" : by not satisfying "startsWith"/"endsWith" comparison
    span = PreprocessorSpanRulesTest.parseSpan("bootestSpanName.1 " +
        "source=spanSourceName-prod " +
        "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 " +
        "key2=val2 1532012145123 1532012146234");
    Assert.assertFalse("Expected [logicalop-reportpoint] rule to return :: false , Actual :: " +
        "true", v2Predicate.test(span));

    // Tests for "none" : by satisfying "contains" comparison
    span = PreprocessorSpanRulesTest.parseSpan("testSpanName.1 " +
        "source=spanSourceName-prod " +
        "spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 " +
        "key2=val2 debug=debug-istrue 1532012145123 1532012146234");
    Assert.assertFalse("Expected [logicalop-reportpoint] rule to return :: false , Actual :: " +
        "true", v2Predicate.test(span));
  }
}
