package com.wavefront.agent.preprocessor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.wavefront.ingester.SpanDecoder;

import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import wavefront.report.Annotation;
import wavefront.report.Span;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PreprocessorSpanRulesTest {

  private static final String FOO = "foo";
  private static final String URL = "url";
  private static final String SOURCE_NAME = "sourceName";
  private static final String SPAN_NAME = "spanName";
  private final PreprocessorRuleMetrics metrics = new PreprocessorRuleMetrics(null, null, null);

  @Test(expected = IllegalArgumentException.class)
  public void testSpanLimitRuleDropSpanNameThrows() {
    new SpanLimitLengthTransformer(SPAN_NAME, 10, LengthLimitActionType.DROP, null, false, metrics);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSpanLimitRuleDropSourceNameThrows() {
    new SpanLimitLengthTransformer(SOURCE_NAME, 10, LengthLimitActionType.DROP, null, false, metrics);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSpanLimitRuleTruncateWithEllipsisMaxLengthLessThan3Throws() {
    new SpanLimitLengthTransformer("parent", 1, LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS, null, false, metrics);
  }

  @Test
  public void testSpanLimitRule() {
    String spanLine = "\"testSpanName\" \"source\"=\"spanSourceName\" " +
        "\"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" \"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" " +
        "\"foo\"=\"bar1-1234567890\" \"foo\"=\"bar2-1234567890\" \"foo\"=\"bar2-2345678901\" \"foo\"=\"baz\" " +
        "1532012145123 1532012146234";
    SpanLimitLengthTransformer rule;
    Span span;

    // ** span name
    // no regex, name gets truncated
    rule = new SpanLimitLengthTransformer(SPAN_NAME, 8, LengthLimitActionType.TRUNCATE, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("testSpan", span.getName());

    // span name matches, gets truncated
    rule = new SpanLimitLengthTransformer(SPAN_NAME, 8, LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS,
        "^test.*", false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(8, span.getName().length());
    assertTrue(span.getName().endsWith("..."));

    // span name does not match, no change
    rule = new SpanLimitLengthTransformer(SPAN_NAME, 8, LengthLimitActionType.TRUNCATE, "nope.*", false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("testSpanName", span.getName());

    // ** source name
    // no regex, source gets truncated
    rule = new SpanLimitLengthTransformer(SOURCE_NAME, 10, LengthLimitActionType.TRUNCATE, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(10, span.getSource().length());

    // source name matches, gets truncated
    rule = new SpanLimitLengthTransformer(SOURCE_NAME, 10, LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS,
        "^spanS.*", false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(10, span.getSource().length());
    assertTrue(span.getSource().endsWith("..."));

    // source name does not match, no change
    rule = new SpanLimitLengthTransformer(SOURCE_NAME, 10, LengthLimitActionType.TRUNCATE, "nope.*", false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("spanSourceName", span.getSource());

    // ** annotations
    // no regex, annotation gets truncated
    rule = new SpanLimitLengthTransformer(FOO, 4, LengthLimitActionType.TRUNCATE, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1", "bar2", "bar2", "baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // no regex, annotations exceeding length limit get dropped
    rule = new SpanLimitLengthTransformer(FOO, 4, LengthLimitActionType.DROP, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // annotation has matches, which get truncated
    rule = new SpanLimitLengthTransformer(FOO, 4, LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS, "bar2-.*", false,
        metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "b...", "b...", "baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // annotation has matches, only first one gets truncated
    rule = new SpanLimitLengthTransformer(FOO, 4, LengthLimitActionType.TRUNCATE, "bar2-.*", true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2", "bar2-2345678901", "baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // annotation has matches, only first one gets dropped
    rule = new SpanLimitLengthTransformer(FOO, 4, LengthLimitActionType.DROP, "bar2-.*", true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2-2345678901", "baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // annotation has no matches, no changes
    rule = new SpanLimitLengthTransformer(FOO, 4, LengthLimitActionType.TRUNCATE, ".*nope.*", false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2-1234567890", "bar2-2345678901", "baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));
  }

  @Test
  public void testSpanAddAnnotationRule() {
    String spanLine = "\"testSpanName\" \"source\"=\"spanSourceName\" " +
        "\"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" \"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" " +
        "\"foo\"=\"bar1-1234567890\" \"foo\"=\"bar2-1234567890\" \"foo\"=\"bar2-2345678901\" \"foo\"=\"baz\" " +
        "1532012145123 1532012146234";
    SpanAddAnnotationTransformer rule;
    Span span;

    rule = new SpanAddAnnotationTransformer(FOO, "baz2", metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2-1234567890", "bar2-2345678901", "baz", "baz2"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));
  }

  @Test
  public void testSpanAddAnnotationIfNotExistsRule() {
    String spanLine = "\"testSpanName\" \"source\"=\"spanSourceName\" " +
        "\"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" \"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" " +
        "\"foo\"=\"bar1-1234567890\" \"foo\"=\"bar2-1234567890\" \"foo\"=\"bar2-2345678901\" \"foo\"=\"baz\" " +
        "1532012145123 1532012146234";
    SpanAddAnnotationTransformer rule;
    Span span;

    rule = new SpanAddAnnotationIfNotExistsTransformer(FOO, "baz2", metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2-1234567890", "bar2-2345678901", "baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    rule = new SpanAddAnnotationIfNotExistsTransformer("foo2", "bar2", metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(5, span.getAnnotations().size());
    assertEquals(new Annotation("foo2", "bar2"), span.getAnnotations().get(4));
  }

  @Test
  public void testSpanDropAnnotationRule() {
    String spanLine = "\"testSpanName\" \"source\"=\"spanSourceName\" " +
        "\"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" \"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" " +
        "\"foo\"=\"bar1-1234567890\" \"foo\"=\"bar2-1234567890\" \"foo\"=\"bar2-2345678901\" \"foo\"=\"baz\" " +
        "1532012145123 1532012146234";
    SpanDropAnnotationTransformer rule;
    Span span;

    // drop first annotation with key = "foo"
    rule = new SpanDropAnnotationTransformer(FOO, null, true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar2-1234567890", "bar2-2345678901", "baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // drop all annotations with key = "foo"
    rule = new SpanDropAnnotationTransformer(FOO, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of(),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // drop all annotations with key = "foo" and value matching bar2.*
    rule = new SpanDropAnnotationTransformer(FOO, "bar2.*", false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // drop first annotation with key = "foo" and value matching bar2.*
    rule = new SpanDropAnnotationTransformer(FOO, "bar2.*", true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2-2345678901", "baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));
  }

  @Test
  public void testSpanExtractAnnotationRule() {
    String spanLine = "testSpanName source=spanSourceName spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
      "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 foo=bar1-1234567890 foo=bar2-2345678901 foo=bar2-3456789012 " +
      "foo=bar boo=baz 1532012145123 1532012146234";
    SpanExtractAnnotationTransformer rule;
    Span span;

    // extract annotation for first value
    rule = new SpanExtractAnnotationTransformer("boo", FOO, "(....)-(.*)$", "$2", "$1", null, true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("baz", "1234567890"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals("boo")).map(Annotation::getValue).
            collect(Collectors.toList()));
    assertEquals(ImmutableList.of("bar1", "bar2-2345678901", "bar2-3456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // extract annotation for first value matching "bar2.*"
    rule = new SpanExtractAnnotationTransformer("boo", FOO, "(....)-(.*)$", "$2", "$1", "bar2.*", true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("baz", "2345678901"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals("boo")).map(Annotation::getValue).
            collect(Collectors.toList()));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2", "bar2-3456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // extract annotation for all values
    rule = new SpanExtractAnnotationTransformer("boo", FOO, "(....)-(.*)$", "$2", "$1", null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("baz", "1234567890", "2345678901", "3456789012"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals("boo")).map(Annotation::getValue).
            collect(Collectors.toList()));
    assertEquals(ImmutableList.of("bar1", "bar2", "bar2", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));
  }

  @Test
  public void testSpanExtractAnnotationIfNotExistsRule() {
    String spanLine = "testSpanName source=spanSourceName spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 foo=bar1-1234567890 foo=bar2-2345678901 foo=bar2-3456789012 " +
        "foo=bar boo=baz 1532012145123 1532012146234";
    SpanExtractAnnotationIfNotExistsTransformer rule;
    Span span;

    // extract annotation for first value
    rule = new SpanExtractAnnotationIfNotExistsTransformer("baz", FOO, "(....)-(.*)$", "$2", "$1", null, true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("1234567890"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals("baz")).map(Annotation::getValue).
            collect(Collectors.toList()));
    assertEquals(ImmutableList.of("bar1", "bar2-2345678901", "bar2-3456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // extract annotation for first value matching "bar2.*
    rule = new SpanExtractAnnotationIfNotExistsTransformer("baz", FOO, "(....)-(.*)$", "$2", "$1", "bar2.*", true,
        metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("2345678901"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals("baz")).map(Annotation::getValue).
            collect(Collectors.toList()));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2", "bar2-3456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // extract annotation for all values
    rule = new SpanExtractAnnotationIfNotExistsTransformer("baz", FOO, "(....)-(.*)$", "$2", "$1", null, false,
        metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("1234567890", "2345678901", "3456789012"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals("baz")).map(Annotation::getValue).
            collect(Collectors.toList()));
    assertEquals(ImmutableList.of("bar1", "bar2", "bar2", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // annotation key already exists, should remain unchanged
    rule = new SpanExtractAnnotationIfNotExistsTransformer("boo", FOO, "(....)-(.*)$", "$2", "$1", null, true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals("boo")).map(Annotation::getValue).
            collect(Collectors.toList()));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2-2345678901", "bar2-3456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // annotation key already exists, should remain unchanged
    rule = new SpanExtractAnnotationIfNotExistsTransformer("boo", FOO, "(....)-(.*)$", "$2", "$1", "bar2.*", true,
        metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals("boo")).map(Annotation::getValue).
            collect(Collectors.toList()));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2-2345678901", "bar2-3456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    // annotation key already exists, should remain unchanged
    rule = new SpanExtractAnnotationIfNotExistsTransformer("boo", FOO, "(....)-(.*)$", "$2", "$1", null, false,
        metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("baz"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals("boo")).map(Annotation::getValue).
            collect(Collectors.toList()));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2-2345678901", "bar2-3456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));
  }

  @Test
  public void testSpanRenameTagRule() {
    String spanLine = "testSpanName source=spanSourceName spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 foo=bar1-1234567890 foo=bar2-2345678901 foo=bar2-3456789012 " +
        "foo=bar boo=baz 1532012145123 1532012146234";
    SpanRenameAnnotationTransformer rule;
    Span span;

    // rename all annotations with key = "foo"
    rule = new SpanRenameAnnotationTransformer(FOO, "foo1", null,
        false,  metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("foo1", "foo1", "foo1", "foo1", "boo"), span.getAnnotations().
        stream().map(Annotation::getKey).collect(Collectors.toList()));

    // rename all annotations with key = "foo" and value matching bar2.*
    rule = new SpanRenameAnnotationTransformer(FOO, "foo1", "bar2.*",
        false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of(FOO, "foo1", "foo1", FOO, "boo"), span.getAnnotations().stream().
        map(Annotation::getKey).
        collect(Collectors.toList()));

    // rename only first annotations with key = "foo" and value matching bar2.*
    rule = new SpanRenameAnnotationTransformer(FOO, "foo1", "bar2.*",
        true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of(FOO, "foo1", FOO, FOO, "boo"), span.getAnnotations().stream().
        map(Annotation::getKey).
        collect(Collectors.toList()));

    // try to rename a annotation whose value doesn't match the regex - shouldn't change
    rule = new SpanRenameAnnotationTransformer(FOO, "foo1", "bar9.*",
        false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of(FOO, FOO, FOO, FOO, "boo"), span.getAnnotations().stream().
        map(Annotation::getKey).
        collect(Collectors.toList()));
  }

  @Test
  public void testSpanForceLowercaseRule() {
    String spanLine = "testSpanName source=spanSourceName spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 foo=BAR1-1234567890 foo=BAR2-2345678901 foo=bAr2-3456789012 " +
        "foo=baR boo=baz 1532012145123 1532012146234";
    SpanForceLowercaseTransformer rule;
    Span span;

    rule = new SpanForceLowercaseTransformer(SOURCE_NAME, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("spansourcename", span.getSource());

    rule = new SpanForceLowercaseTransformer(SPAN_NAME, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("testspanname", span.getName());

    rule = new SpanForceLowercaseTransformer(SPAN_NAME, "test.*", false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("testspanname", span.getName());

    rule = new SpanForceLowercaseTransformer(SPAN_NAME, "nomatch", false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("testSpanName", span.getName());

    rule = new SpanForceLowercaseTransformer(FOO, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2-2345678901", "bar2-3456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    rule = new SpanForceLowercaseTransformer(FOO, null, true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "BAR2-2345678901", "bAr2-3456789012", "baR"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    rule = new SpanForceLowercaseTransformer(FOO, "BAR.*", false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2-2345678901", "bAr2-3456789012", "baR"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    rule = new SpanForceLowercaseTransformer(FOO, "no_match", false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("BAR1-1234567890", "BAR2-2345678901", "bAr2-3456789012", "baR"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));
  }

  @Test
  public void testSpanReplaceRegexRule() {
    String spanLine = "testSpanName source=spanSourceName spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 foo=bar1-1234567890 foo=bar2-2345678901 foo=bar2-3456789012 " +
        "foo=bar boo=baz url=\"https://localhost:50051/style/foo/make?id=5145\" " +
        "1532012145123 1532012146234";
    SpanReplaceRegexTransformer rule;
    Span span;

    rule = new SpanReplaceRegexTransformer(SPAN_NAME, "test", "", null, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("SpanName", span.getName());

    rule = new SpanReplaceRegexTransformer(SOURCE_NAME, "Name", "Z", null, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("spanSourceZ", span.getSource());

    rule = new SpanReplaceRegexTransformer(SOURCE_NAME, "Name", "Z", "span.*", null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("spanSourceZ", span.getSource());

    rule = new SpanReplaceRegexTransformer(SOURCE_NAME, "Name", "Z", "no_match", null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("spanSourceName", span.getSource());

    rule = new SpanReplaceRegexTransformer(FOO, "234", "zzz", null, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1zzz567890", "bar2-zzz5678901", "bar2-3456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    rule = new SpanReplaceRegexTransformer(FOO, "901", "zzz", null, null, true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar1-1234567890", "bar2-2345678zzz", "bar2-3456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    rule = new SpanReplaceRegexTransformer(FOO, "\\d-\\d", "@", null, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar@234567890", "bar@345678901", "bar@456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    rule = new SpanReplaceRegexTransformer(FOO, "\\d-\\d", "@", null, null, true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("bar@234567890", "bar2-2345678901", "bar2-3456789012", "bar"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(FOO)).map(Annotation::getValue).
            collect(Collectors.toList()));

    rule = new SpanReplaceRegexTransformer(URL, "(https:\\/\\/.+\\/style\\/foo\\/make\\?id=)(.*)",
        "$1REDACTED", null, null, true, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals(ImmutableList.of("https://localhost:50051/style/foo/make?id=REDACTED"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals(URL)).map(Annotation::getValue).
            collect(Collectors.toList()));

    rule = new SpanReplaceRegexTransformer("boo", "^.*$", "{{foo}}-{{spanName}}-{{sourceName}}{{}}",
        null, null, false, metrics);
    span = rule.apply(parseSpan(spanLine));
    assertEquals("bar1-1234567890-testSpanName-spanSourceName{{}}", span.getAnnotations().stream().
        filter(x -> x.getKey().equals("boo")).map(Annotation::getValue).findFirst().orElse("fail"));
  }

  @Test
  public void testSpanWhitelistBlacklistRules() {
    String spanLine = "testSpanName source=spanSourceName spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
        "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 foo=bar1-1234567890 foo=bar2-2345678901 foo=bar2-3456789012 " +
        "foo=bar boo=baz url=\"https://localhost:50051/style/foo/make?id=5145\" " +
        "1532012145123 1532012146234";
    SpanBlacklistRegexFilter blacklistRule;
    SpanWhitelistRegexFilter whitelistRule;
    Span span = parseSpan(spanLine);

    blacklistRule = new SpanBlacklistRegexFilter(SPAN_NAME, "^test.*$", metrics);
    whitelistRule = new SpanWhitelistRegexFilter(SPAN_NAME, "^test.*$", metrics);
    assertFalse(blacklistRule.test(span));
    assertTrue(whitelistRule.test(span));

    blacklistRule = new SpanBlacklistRegexFilter(SPAN_NAME, "^ztest.*$", metrics);
    whitelistRule = new SpanWhitelistRegexFilter(SPAN_NAME, "^ztest.*$", metrics);
    assertTrue(blacklistRule.test(span));
    assertFalse(whitelistRule.test(span));

    blacklistRule = new SpanBlacklistRegexFilter(SOURCE_NAME, ".*ourceN.*", metrics);
    whitelistRule = new SpanWhitelistRegexFilter(SOURCE_NAME, ".*ourceN.*", metrics);
    assertFalse(blacklistRule.test(span));
    assertTrue(whitelistRule.test(span));

    blacklistRule = new SpanBlacklistRegexFilter(SOURCE_NAME, "ourceN.*", metrics);
    whitelistRule = new SpanWhitelistRegexFilter(SOURCE_NAME, "ourceN.*", metrics);
    assertTrue(blacklistRule.test(span));
    assertFalse(whitelistRule.test(span));

    blacklistRule = new SpanBlacklistRegexFilter("foo", "bar", metrics);
    whitelistRule = new SpanWhitelistRegexFilter("foo", "bar", metrics);
    assertFalse(blacklistRule.test(span));
    assertTrue(whitelistRule.test(span));

    blacklistRule = new SpanBlacklistRegexFilter("foo", "baz", metrics);
    whitelistRule = new SpanWhitelistRegexFilter("foo", "baz", metrics);
    assertTrue(blacklistRule.test(span));
    assertFalse(whitelistRule.test(span));
  }

  @Test
  public void testSpanSanitizeTransformer() {
    Span span = Span.newBuilder().setCustomer("dummy").setStartMillis(System.currentTimeMillis())
        .setDuration(2345)
        .setName(" HTT*P GET\"\n? ")
        .setSource("'customJaegerSource'")
        .setSpanId("00000000-0000-0000-0000-00000023cace")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        .setAnnotations(ImmutableList.of(
            new Annotation("service", "frontend"),
            new Annotation("special|tag:", "''"),
            new Annotation("specialvalue", " hello \n world ")))
        .build();
    SpanSanitizeTransformer transformer = new SpanSanitizeTransformer(metrics);
    span = transformer.apply(span);
    assertEquals("HTT-P GET\"\\n?", span.getName());
    assertEquals("-customJaegerSource-", span.getSource());
    assertEquals(ImmutableList.of("''"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals("special-tag-")).map(Annotation::getValue).
            collect(Collectors.toList()));
    assertEquals(ImmutableList.of("hello \\n world"),
        span.getAnnotations().stream().filter(x -> x.getKey().equals("specialvalue")).map(Annotation::getValue).
            collect(Collectors.toList()));
  }

  private Span parseSpan(String line) {
    List<Span> out = Lists.newArrayListWithExpectedSize(1);
    new SpanDecoder("unknown").decode(line, out, "dummy");
    return out.get(0);
  }
}
