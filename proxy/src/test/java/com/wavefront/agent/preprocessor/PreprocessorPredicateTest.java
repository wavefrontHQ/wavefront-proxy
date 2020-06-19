package com.wavefront.agent.preprocessor;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import wavefront.report.ReportPoint;

public class PreprocessorPredicateTest {

  @Test
  public void test() {
    String predicateString =         "\"{{sourceName}}\" contains \"prod\" and " +
        "{{metricName}} startsWith \"foometric.\" or " +
        "{{metricName}} startsWith \"foometric.\" or {{env}} = \"prod\"" +
        " and (\"{{sourceName}}-eh\" startsWith(\"foo\"))";
    PreprocessorUtil.parsePredicateString(predicateString);
  }

  @Test
  public void test2() {
    PreprocessorUtil.parsePredicateString("({{sourceName}} contains(\"prod\") and " +
        "({{metricName}}" +
        " startsWith(\"foometric.\") or {{metricName}} startsWith (\"foometric.\") or {{env}} = " +
        "(\"prod\") and (\"{{sourceName}}-eh\" startsWith (\"foo\")))) and " +
        "$timestamp / 1000 > time('10 minutes ago') + 900M");

    ReportPoint point = ReportPoint.newBuilder().setTable("test").setValue(1234.0).
        setTimestamp(System.currentTimeMillis() - 30 * 1000).setMetric("test").setHost("host").
        setAnnotations(ImmutableMap.of("level", "5a")).build();
    boolean testResult =
          PreprocessorUtil.parsePredicateString(
              "parse(\"{{level}}\".replaceAll(\"a\", \"\"), 4) = 5 ").test(point);
    PreprocessorUtil.parsePredicateString("$timestamp > time('31 seconds ago')").test(point);
    System.out.println("Test: " + testResult);
  }
}
