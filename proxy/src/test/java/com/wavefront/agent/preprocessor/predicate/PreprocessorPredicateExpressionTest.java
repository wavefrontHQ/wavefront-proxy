package com.wavefront.agent.preprocessor.predicate;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import wavefront.report.ReportPoint;

import static com.wavefront.agent.preprocessor.predicate.Predicates.parseEvalExpression;
import static org.junit.Assert.assertEquals;

/**
 * @author vasily@wavefront.com
 */
public class PreprocessorPredicateExpressionTest {

  @Test
  public void testMath() {
    assertEq(4, parseEvalExpression("2 + 2").getValue(null));
    assertEq(6, parseEvalExpression("2 * 3").getValue(null));
    assertEq(1, parseEvalExpression("3 - 2").getValue(null));
    assertEq(2.5, parseEvalExpression("10 / 4").getValue(null));
    assertEq(1.5, parseEvalExpression("10.5 % 3").getValue(null));
    assertEq(0x8800, parseEvalExpression("0x8000 + 0x800").getValue(null));
    assertEq(0x8888, parseEvalExpression("0x8080 | 0x808").getValue(null));
    assertEq(0, parseEvalExpression("0x8080 & 0x808").getValue(null));
    assertEq(0xFF0F, parseEvalExpression("0xF0F0 ^ 0xFFF").getValue(null));
    assertEq(0xFFFFF765, parseEvalExpression("~0x89A").getValue(null));
    assertEq(5, parseEvalExpression("10 >> 1").getValue(null));
    assertEq(-5, parseEvalExpression("-10 >> 1").getValue(null));
    assertEq(5, parseEvalExpression("10 >>> 1").getValue(null));
    assertEq(9223372036854775803L, parseEvalExpression("-10 >>> 1").getValue(null));
    assertEq(20, parseEvalExpression("5 << 2").getValue(null));
    assertEq(-40, parseEvalExpression("-10 << 2").getValue(null));
    assertEq(20, parseEvalExpression("5 <<< 2").getValue(null));
    assertEq(-40, parseEvalExpression("-10 <<< 2").getValue(null));
    assertEq(5, parseEvalExpression("10 >>> 1").getValue(null));
    assertEq(6, parseEvalExpression("2 + 2 * 2").getValue(null));
    assertEq(6, parseEvalExpression("2 + (2 * 2)").getValue(null));
    assertEq(8, parseEvalExpression("(2 + 2) * 2").getValue(null));
    assertEq(0, parseEvalExpression("1 = 2").getValue(null));
    assertEq(1, parseEvalExpression("1 = 1").getValue(null));
    assertEq(1, parseEvalExpression("1 != 2").getValue(null));
    assertEq(0, parseEvalExpression("1 != 1").getValue(null));
    assertEq(0, parseEvalExpression("2 + 2 = 5").getValue(null));
    assertEq(1, parseEvalExpression("2 + 2 = 4").getValue(null));
    assertEq(1, parseEvalExpression("2 + 2 > 3").getValue(null));
    assertEq(1, parseEvalExpression("2 + 2 >= 4").getValue(null));
    assertEq(1, parseEvalExpression("2 + 2 <= 4").getValue(null));
    assertEq(0, parseEvalExpression("2 + 2 > 4").getValue(null));
    assertEq(1, parseEvalExpression("3 < 2 + 2 < 5").getValue(null));
    assertEq(0, parseEvalExpression("4 < 2 + 2").getValue(null));
    assertEq(1, parseEvalExpression("(1 = 1) and (2 = 2)").getValue(null));
    assertEq(0, parseEvalExpression("(1 = 1) and (2 != 2)").getValue(null));
    assertEq(0, parseEvalExpression("(1 != 1) and (2 = 2)").getValue(null));
    assertEq(0, parseEvalExpression("(1 != 1) and (2 != 2)").getValue(null));
    assertEq(1, parseEvalExpression("(1 = 1) or (2 = 2)").getValue(null));
    assertEq(1, parseEvalExpression("(1 = 1) or (2 != 2)").getValue(null));
    assertEq(1, parseEvalExpression("(1 != 1) or (2 = 2)").getValue(null));
    assertEq(1, parseEvalExpression("5 and 2").getValue(null));
    assertEq(0, parseEvalExpression("5 and 0").getValue(null));
    assertEq(0, parseEvalExpression("0 and 5").getValue(null));
    assertEq(0, parseEvalExpression("0 and 0").getValue(null));
    assertEq(1, parseEvalExpression("5 or 2").getValue(null));
    assertEq(1, parseEvalExpression("5 or 0").getValue(null));
    assertEq(1, parseEvalExpression("0 or 5").getValue(null));
    assertEq(0, parseEvalExpression("0 or 0").getValue(null));
    assertEq(1, parseEvalExpression("not 0").getValue(null));
    assertEq(0, parseEvalExpression("not 1").getValue(null));
    assertEq(0, parseEvalExpression("not 5").getValue(null));
    assertEq(0, parseEvalExpression("(1 != 1) and (2 != 2)").getValue(null));
    assertEq(1, parseEvalExpression("random() > 0").getValue(null));
    assertEq(1, parseEvalExpression("random() < 1").getValue(null));
  }

  @Test
  public void testStringComparison() {
    assertEq(1, parseEvalExpression("'aaa' = 'aaa'").getValue(null));
    assertEq(0, parseEvalExpression("'aaa' = 'aa'").getValue(null));
    assertEq(1, parseEvalExpression("'aaa' equals 'aaa'").getValue(null));
    assertEq(0, parseEvalExpression("'aaa' equals 'aa'").getValue(null));
    assertEq(1, parseEvalExpression("'aAa' equalsIgnoreCase 'AaA'").getValue(null));
    assertEq(0, parseEvalExpression("'aAa' equalsIgnoreCase 'Aa'").getValue(null));
    assertEq(1, parseEvalExpression("'aaa' contains 'aa'").getValue(null));
    assertEq(0, parseEvalExpression("'aaa' contains 'ab'").getValue(null));
    assertEq(1, parseEvalExpression("'aAa' containsIgnoreCase 'aa'").getValue(null));
    assertEq(1, parseEvalExpression("'aAa' containsIgnoreCase 'AA'").getValue(null));
    assertEq(0, parseEvalExpression("'aAa' containsIgnoreCase 'ab'").getValue(null));
    assertEq(1, parseEvalExpression("'abcd' startsWith 'ab'").getValue(null));
    assertEq(0, parseEvalExpression("'abcd' startsWith 'cd'").getValue(null));
    assertEq(1, parseEvalExpression("'aBCd' startsWithIgnoreCase 'Ab'").getValue(null));
    assertEq(0, parseEvalExpression("'aBCd' startsWithIgnoreCase 'Cd'").getValue(null));
    assertEq(1, parseEvalExpression("'abcd' endsWith 'cd'").getValue(null));
    assertEq(0, parseEvalExpression("'abcd' endsWith 'ab'").getValue(null));
    assertEq(1, parseEvalExpression("'aBCd' endsWithIgnoreCase 'cD'").getValue(null));
    assertEq(0, parseEvalExpression("'aBCd' endsWithIgnoreCase 'aB'").getValue(null));
    assertEq(1, parseEvalExpression("'abcde' matches '^.+bc.+$'").getValue(null));
    assertEq(0, parseEvalExpression("'abcde' matches '^.+de.+$'").getValue(null));
    assertEq(1, parseEvalExpression("'abCDe' matchesIgnoreCase '^.+bc.+$'").getValue(null));
    assertEq(0, parseEvalExpression("'abCDe' matchesIgnoreCase '^.+de.+$'").getValue(null));
    assertEq(1, parseEvalExpression("'abcde' regexMatch '^.+bc.+$'").getValue(null));
    assertEq(0, parseEvalExpression("'abcde' regexMatch '^.+de.+$'").getValue(null));
    assertEq(1, parseEvalExpression("'abCDe' regexMatchIgnoreCase '^.+bc.+$'").getValue(null));
    assertEq(0, parseEvalExpression("'abCDe' regexMatchIgnoreCase '^.+de.+$'").getValue(null));
    assertEq(1, parseEvalExpression("'bc' in ('ab', 'bc', 'cd')").getValue(null));
    assertEq(0, parseEvalExpression("'de' in ('ab', 'bc', 'cd')").getValue(null));
  }

  @Test
  public void testStringFunc() {
    assertEq(1, parseEvalExpression("'abc' + 'def' = 'abcdef'").getValue(null));
    assertEq(1, parseEvalExpression("'abc' + 'def' = 'ab' + 'cdef'").getValue(null));
    assertEq(1, parseEvalExpression("('abc' + 'def') = ('abcdef')").getValue(null));
    assertEq(1, parseEvalExpression("'abcdef'.left(3) = 'abc'").getValue(null));
    assertEq(1, parseEvalExpression("'abcdef'.right(3) = 'def'").getValue(null));
    assertEq(1, parseEvalExpression("'abcdef'.substring(4) = 'ef'").getValue(null));
    assertEq(1, parseEvalExpression("'abcdef'.substring(3, 5) = 'de'").getValue(null));
    assertEq(1, parseEvalExpression("'aBcDeF'.toLowerCase() = 'abcdef'").getValue(null));
    assertEq(1, parseEvalExpression("'aBcDeF'.toUpperCase() = 'ABCDEF'").getValue(null));
    assertEq(1, parseEvalExpression("'abcdef'.replace('de', '') = 'abcf'").getValue(null));
    assertEq(1, parseEvalExpression("'a1b2c3d4'.replaceAll('\\d', '') = 'abcd'").getValue(null));
    assertEq(1, parseEvalExpression("isBlank('')").getValue(null));
    assertEq(1, parseEvalExpression("isBlank(' ')").getValue(null));
    assertEq(0, parseEvalExpression("isBlank('abc')").getValue(null));
    assertEq(1, parseEvalExpression("isEmpty('')").getValue(null));
    assertEq(0, parseEvalExpression("isEmpty(' ')").getValue(null));
    assertEq(0, parseEvalExpression("isEmpty('abc')").getValue(null));
    assertEq(0, parseEvalExpression("isNotBlank('')").getValue(null));
    assertEq(0, parseEvalExpression("isNotBlank(' ')").getValue(null));
    assertEq(1, parseEvalExpression("isNotBlank('abc')").getValue(null));
    assertEq(0, parseEvalExpression("isNotEmpty('')").getValue(null));
    assertEq(1, parseEvalExpression("isNotEmpty(' ')").getValue(null));
    assertEq(1, parseEvalExpression("isNotEmpty('abc')").getValue(null));
    assertEq(4, parseEvalExpression("length('abcd')").getValue(null));
    assertEq(1139631978, parseEvalExpression("hashCode('abcd')").getValue(null));
    assertEq(12345, parseEvalExpression("parse('12345')").getValue(null));
    assertEq(12345, parseEvalExpression("parse('123a45'.replace('a', ''))").getValue(null));
  }

  @Test
  public void testIff() {
    assertEq(5, parseEvalExpression("if(2 = 2, 5, 6)").getValue(null));
    assertEq(6, parseEvalExpression("if(2 != 2, 5, 6)").getValue(null));
    assertEq(1, parseEvalExpression("if(2 = 2, 'abc', 'def') = 'abc'").getValue(null));
    assertEq(1, parseEvalExpression("if(2 != 2, 'abc', 'def') = 'def'").getValue(null));
  }

  @Test
  public void test() {
    /*
    String predicateString =         "\"{{sourceName}}\" contains \"prod\" and " +
        "{{metricName}} startsWith \"foometric.\" or " +
        "{{metricName}} startsWith \"foometric.\" or {{env}} = \"prod\"" +
        " and (\"{{sourceName}}-eh\" startsWith(\"foo\"))";
    parseEvalExpression(predicateString, System::currentTimeMillis);
     */
  }

  @Test
  public void test2() {
    parseEvalExpression("({{sourceName}} contains(\"prod\") and " +
        "({{metricName}}" +
        " startsWith(\"foometric.\") or {{metricName}} startsWith (\"foometric.\") or {{env}} = " +
        "(\"prod\") and (\"{{sourceName}}-eh\" startsWith (\"foo\")))) and " +
        "$timestamp / 1000 > time('10 minutes ago') + 900M", System::currentTimeMillis);

    ReportPoint point = ReportPoint.newBuilder().setTable("test").setValue(1234.0).
        setTimestamp(System.currentTimeMillis() - 30 * 1000).setMetric("test").setHost("host").
        setAnnotations(ImmutableMap.of("level", "5a")).build();

    parseEvalExpression("parse(\"{{level}}\".replaceAll(\"a\", \"\"), 4) = 5 ");
    parseEvalExpression("$timestamp > time('31 seconds ago')");
  }
  
  private static void assertEq(double d1, double d2) {
    assertEquals(d1, d2, 1e-6);
  }
}
