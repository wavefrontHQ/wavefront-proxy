package com.wavefront.agent.preprocessor.predicate;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import wavefront.report.Annotation;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.wavefront.agent.preprocessor.predicate.Predicates.parseEvalExpression;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
  public void testUnits() {
    assertEq(0.000000000000000000000005, parseEvalExpression("5y").getValue(null));
    assertEq(0.000000000000000000005, parseEvalExpression("5z").getValue(null));
    assertEq(0.000000000000000005, parseEvalExpression("5a").getValue(null));
    assertEq(0.000000000000005, parseEvalExpression("5f").getValue(null));
    assertEq(0.000000000005, parseEvalExpression("5p").getValue(null));
    assertEq(0.000000005, parseEvalExpression("5n").getValue(null));
    assertEq(0.000005, parseEvalExpression("5µ").getValue(null));
    assertEq(0.005, parseEvalExpression("5m").getValue(null));
    assertEq(0.05, parseEvalExpression("5c").getValue(null));
    assertEq(0.5, parseEvalExpression("5d").getValue(null));
    assertEq(50, parseEvalExpression("5da").getValue(null));
    assertEq(500, parseEvalExpression("5h").getValue(null));
    assertEq(5_000, parseEvalExpression("5k").getValue(null));
    assertEq(5_000_000, parseEvalExpression("5M").getValue(null));
    assertEq(5_000_000_000L, parseEvalExpression("5G").getValue(null));
    assertEq(5_000_000_000_000L, parseEvalExpression("5T").getValue(null));
    assertEq(5_000_000_000_000_000L, parseEvalExpression("5P").getValue(null));
    assertEq(5_000_000_000_000_000_000L, parseEvalExpression("5E").getValue(null));
    assertEq(5e21, parseEvalExpression("5Z").getValue(null));
    assertEquals(5e24, parseEvalExpression("5Y").getValue(null), 1.1e9);
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
    assertEq(112566101, parseEvalExpression("hashCode('utf8-тест')").getValue(null));
    assertEq(12345, parseEvalExpression("parse('12345')").getValue(null));
    assertEq(123, parseEvalExpression("parse('123a45', 123)").getValue(null));
    assertEq(0, parseEvalExpression("parse('123a45')").getValue(null));
    assertEq(12345, parseEvalExpression("parse('123a45'.replace('a', ''), 123)").getValue(null));
  }

  @Test
  public void testStringEvalFunc() {
    assertEq(1, parseEvalExpression("''.isBlank()").getValue(null));
    assertEq(1, parseEvalExpression("' '.isBlank()").getValue(null));
    assertEq(0, parseEvalExpression("'abc'.isBlank()").getValue(null));
    assertEq(1, parseEvalExpression("''.isEmpty()").getValue(null));
    assertEq(0, parseEvalExpression("' '.isEmpty()").getValue(null));
    assertEq(0, parseEvalExpression("'abc'.isEmpty()").getValue(null));
    assertEq(0, parseEvalExpression("''.isNotBlank()").getValue(null));
    assertEq(0, parseEvalExpression("' '.isNotBlank()").getValue(null));
    assertEq(1, parseEvalExpression("'abc'.isNotBlank()").getValue(null));
    assertEq(0, parseEvalExpression("''.isNotEmpty()").getValue(null));
    assertEq(1, parseEvalExpression("' '.isNotEmpty()").getValue(null));
    assertEq(1, parseEvalExpression("'abc'.isNotEmpty()").getValue(null));
    assertEq(4, parseEvalExpression("'abcd'.length()").getValue(null));
    assertEq(12345, parseEvalExpression("'12345'.parse()").getValue(null));
    assertEq(0, parseEvalExpression("'12345-a'.parse()").getValue(null));
    assertEq(123, parseEvalExpression("'12345-a'.parse(123)").getValue(null));
    assertEq(1139631978, parseEvalExpression("'abcd'.hashCode()").getValue(null));
  }

  @Test
  public void testIff() {
    assertEq(5, parseEvalExpression("if(2 = 2, 5, 6)").getValue(null));
    assertEq(6, parseEvalExpression("if(2 != 2, 5, 6)").getValue(null));
    assertEq(1, parseEvalExpression("if(2 = 2, 'abc', 'def') = 'abc'").getValue(null));
    assertEq(1, parseEvalExpression("if(2 != 2, 'abc', 'def') = 'def'").getValue(null));
  }

  @Test
  public void testPointExpression() {
    ReportPoint point = ReportPoint.newBuilder().
        setTable("test").
        setValue(1234.5).
        setTimestamp(1592837162000L).
        setMetric("testMetric").
        setHost("testHost").
        setAnnotations(ImmutableMap.of("tagk1", "tagv1", "tagk2", "tagv2",
            "env", "prod", "dc", "us-west-2")).
        build();
    assertEq(1, parseEvalExpression("$value = 1234.5").getValue(point));
    assertEq(0, parseEvalExpression("$value = 1234.0").getValue(point));
    assertEq(1, parseEvalExpression("$timestamp = 1592837162000").getValue(point));
    assertEq(1, parseEvalExpression("{{sourceName}} contains 'test'").getValue(point));
    assertEq(1, parseEvalExpression("'{{sourceName}}' contains 'test'").getValue(point));
    assertEq(1, parseEvalExpression("\"{{sourceName}}\" contains 'test'").getValue(point));
    assertEq(0, parseEvalExpression("{{sourceName}} contains 'sourceName'").getValue(point));
    assertEq(1, parseEvalExpression("{{sourceName}} contains 'sourceName'").getValue(null));
    assertEq(1, parseEvalExpression("{{metricName}} contains 'test'").getValue(point));
    assertEq(1, parseEvalExpression("{{sourceName}} all startsWith 'test'").getValue(point));
    assertEq(1, parseEvalExpression("{{metricName}} all startsWith 'test'").getValue(point));
    assertEq(1, parseEvalExpression("{{tagk1}} equals 'tagv1'").getValue(point));
    assertEq(1, parseEvalExpression("{{tagk1}} all equals 'tagv1'").getValue(point));
    assertEq(0, parseEvalExpression("{{tagk1}} all equals 'tagv1'").getValue(null));
    assertEq(1, parseEvalExpression("parse({{tagk2}}.replace('tagv', ''), 3) = 2").getValue(point));
    assertEq(1, parseEvalExpression("{{doesNotExist}}.isEmpty()").getValue(point));
    assertEq(1, parseEvalExpression("\"{{env}}:{{dc}}\" equals 'prod:us-west-2'").getValue(point));
    assertEq(1, parseEvalExpression("$timestamp < time('now')").getValue(point));
    assertEq(0, parseEvalExpression("$timestamp > time('31 seconds ago')").getValue(point));
    assertEq(1, parseEvalExpression("$timestamp < time('2020-06-23', 'UTC')").getValue(point));
    try {
      parseEvalExpression("$startMillis > 0").getValue(point);
      fail();
    } catch (ClassCastException cce) {
      // pass
    }
    try {
      parseEvalExpression("$duration > 0").getValue(point);
      fail();
    } catch (ClassCastException cce) {
      // pass
    }
  }

  @Test
  public void testSpanExpression() {
    Span span = Span.newBuilder().
        setCustomer("test").
        setName("testSpanName").
        setSource("spanSourceName").
        setSpanId("4217104a-690d-4927-baff-d9aa779414c2").
        setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0").
        setAnnotations(ImmutableList.of(
            new Annotation("foo", "bar1-baz"),
            new Annotation("foo", "bar2-baz"),
            new Annotation("boo", "baz"))).
        setStartMillis(1532012145123L).
        setDuration(1111).
        build();
    assertEq(1, parseEvalExpression("$duration = 1111").getValue(span));
    assertEq(0, parseEvalExpression("$duration = 1111.1").getValue(span));
    assertEq(1, parseEvalExpression("$startMillis = 1532012145123").getValue(span));
    assertEq(1, parseEvalExpression("{{sourceName}} startsWith 'span'").getValue(span));
    assertEq(1, parseEvalExpression("'{{sourceName}}' startsWith 'span'").getValue(span));
    assertEq(1, parseEvalExpression("\"{{sourceName}}\" startsWith 'span'").getValue(span));
    assertEq(0, parseEvalExpression("{{sourceName}} contains 'sourceName'").getValue(span));
    assertEq(1, parseEvalExpression("{{sourceName}} contains 'sourceName'").getValue(null));
    assertEq(1, parseEvalExpression("{{spanName}} startsWith 'test'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} equals 'bar1-baz'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} all startsWith 'bar'").getValue(span));
    assertEq(0, parseEvalExpression("{{foo}} all startsWith 'bar1'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} any startsWith 'bar1'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} any startsWith 'bar2'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} none startsWith 'bar3'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} all endsWith 'baz'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} all endsWithIgnoreCase 'BAZ'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} all startsWithIgnoreCase 'BAR'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} all contains '-'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} all containsIgnoreCase '-BA'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} all matches '^bar.*$'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} all matchesIgnoreCase '^BAR.*$'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} any equals 'bar2-baz'").getValue(span));
    assertEq(1, parseEvalExpression("{{foo}} any equalsIgnoreCase 'bar2-BAZ'").getValue(span));
    assertEq(1, parseEvalExpression("{{sourceName}} all startsWith 'span'").getValue(span));
    assertEq(1, parseEvalExpression("{{spanName}} all startsWith 'test'").getValue(span));

    try {
      parseEvalExpression("$timestamp > 0").getValue(span);
      fail();
    } catch (ClassCastException cce) {
      // pass
    }
    try {
      parseEvalExpression("$value > 0").getValue(span);
      fail();
    } catch (ClassCastException cce) {
      // pass
    }
  }

  private static void assertEq(double d1, double d2) {
    assertEquals(d1, d2, 1e-12);
  }
}
