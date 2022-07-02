package com.wavefront.agent.preprocessor;

import static com.wavefront.agent.preprocessor.LengthLimitActionType.TRUNCATE;
import static com.wavefront.agent.preprocessor.LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

public class PreprocessorLogRulesTest {
  private static PreprocessorConfigManager config;
  private final PreprocessorRuleMetrics metrics = new PreprocessorRuleMetrics(null, null, null);

  @BeforeClass
  public static void setup() throws IOException {
    InputStream stream =
        PreprocessorRulesTest.class.getResourceAsStream("log_preprocessor_rules" + ".yaml");
    config = new PreprocessorConfigManager();
    config.loadFromStream(stream);
  }

  /** tests that valid rules are successfully loaded */
  @Test
  public void testReportLogLoadValidRules() {
    // Arrange
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    InputStream stream =
        PreprocessorRulesTest.class.getResourceAsStream("log_preprocessor_rules" + ".yaml");

    // Act
    config.loadFromStream(stream);

    // Assert
    Assert.assertEquals(0, config.totalInvalidRules);
    Assert.assertEquals(22, config.totalValidRules);
  }

  /** tests that ReportLogAddTagIfNotExistTransformer successfully adds tags */
  @Test
  public void testReportLogAddTagIfNotExistTransformer() {
    // Arrange
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("")
            .setMessage("")
            .setAnnotations(new ArrayList<>())
            .build();
    ReportLogAddTagIfNotExistsTransformer rule =
        new ReportLogAddTagIfNotExistsTransformer("foo", "bar", null, metrics);

    // Act
    rule.apply(log);

    // Assert
    assertEquals(1, log.getAnnotations().size());
    assertTrue(log.getAnnotations().contains(new Annotation("foo", "bar")));
  }

  /** tests that ReportLogAddTagTransformer successfully adds tags */
  @Test
  public void testReportLogAddTagTransformer() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("")
            .setMessage("")
            .setAnnotations(existingAnnotations)
            .build();

    ReportLogAddTagTransformer rule = new ReportLogAddTagTransformer("foo", "bar2", null, metrics);

    // Act
    rule.apply(log);

    // Assert
    assertEquals(2, log.getAnnotations().size());
    assertTrue(log.getAnnotations().contains(new Annotation("foo", "bar2")));
  }

  /** tests that creating a new ReportLogAllowFilter with no v2 predicates works as expected */
  @Test
  public void testReportLogAllowFilter_CreateNoV2Predicates() {
    try {
      new ReportLogAllowFilter("name", null, null, metrics);
    } catch (NullPointerException e) {
      // expected
    }

    try {
      new ReportLogAllowFilter(null, "match", null, metrics);
    } catch (NullPointerException e) {
      // expected
    }

    try {
      new ReportLogAllowFilter("name", "", null, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new ReportLogAllowFilter("", "match", null, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    new ReportLogAllowFilter("name", "match", null, metrics);
  }

  /** tests that creating a new ReportLogAllowFilter with v2 predicates works as expected */
  @Test
  public void testReportLogAllowFilters_CreateV2PredicatesExists() {
    try {
      new ReportLogAllowFilter("name", null, x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new ReportLogAllowFilter(null, "match", x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new ReportLogAllowFilter("name", "", x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new ReportLogAllowFilter("", "match", x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    new ReportLogAllowFilter("name", "match", x -> false, metrics);
    new ReportLogAllowFilter(null, null, x -> false, metrics);
  }

  /** tests that new ReportLogAllowFilter allows all logs it should */
  @Test
  public void testReportLogAllowFilters_testAllowed() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("abc")
            .setMessage("123")
            .setAnnotations(existingAnnotations)
            .build();

    ReportLogAllowFilter filter1 =
        new ReportLogAllowFilter("message", "^[0-9]+", x -> true, metrics);
    ReportLogAllowFilter filter2 =
        new ReportLogAllowFilter("sourceName", "^[a-z]+", x -> true, metrics);
    ReportLogAllowFilter filter3 = new ReportLogAllowFilter("foo", "bar", x -> true, metrics);
    ReportLogAllowFilter filter4 = new ReportLogAllowFilter(null, null, x -> true, metrics);

    // Act
    boolean isAllowed1 = filter1.test(log);
    boolean isAllowed2 = filter2.test(log);
    boolean isAllowed3 = filter3.test(log);
    boolean isAllowed4 = filter4.test(log);

    // Assert
    assertTrue(isAllowed1);
    assertTrue(isAllowed2);
    assertTrue(isAllowed3);
    assertTrue(isAllowed4);
  }

  /** tests that new ReportLogAllowFilter rejects all logs it should */
  @Test
  public void testReportLogAllowFilters_testNotAllowed() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("abc")
            .setMessage("123")
            .setAnnotations(existingAnnotations)
            .build();

    ReportLogAllowFilter filter1 = new ReportLogAllowFilter(null, null, x -> false, metrics);
    ReportLogAllowFilter filter2 = new ReportLogAllowFilter("sourceName", "^[0-9]+", null, metrics);
    ReportLogAllowFilter filter3 = new ReportLogAllowFilter("message", "^[a-z]+", null, metrics);
    ReportLogAllowFilter filter4 = new ReportLogAllowFilter("foo", "bar2", null, metrics);

    // Act
    boolean isAllowed1 = filter1.test(log);
    boolean isAllowed2 = filter2.test(log);
    boolean isAllowed3 = filter3.test(log);
    boolean isAllowed4 = filter4.test(log);

    // Assert
    assertFalse(isAllowed1);
    assertFalse(isAllowed2);
    assertFalse(isAllowed3);
    assertFalse(isAllowed4);
  }

  /** tests that ReportLogAllowTagTransformer successfully removes tags not allowed */
  @Test
  public void testReportLogAllowTagTransformer() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
    existingAnnotations.add(Annotation.newBuilder().setKey("k1").setValue("v1").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("abc")
            .setMessage("123")
            .setAnnotations(existingAnnotations)
            .build();

    Map<String, String> allowedTags = new HashMap<>();
    allowedTags.put("k1", "v1");
    ReportLogAllowTagTransformer rule =
        new ReportLogAllowTagTransformer(allowedTags, null, metrics);

    // Act
    rule.apply(log);
    // Assert
    assertEquals(log.getAnnotations().size(), 1);
    assertTrue(log.getAnnotations().contains(new Annotation("k1", "v1")));
  }

  /** tests that creating a new ReportLogBlockFilter with no v2 predicates works as expected */
  @Test
  public void testReportLogBlockFilter_CreateNoV2Predicates() {
    try {
      new ReportLogBlockFilter("name", null, null, metrics);
    } catch (NullPointerException e) {
      // expected
    }

    try {
      new ReportLogBlockFilter(null, "match", null, metrics);
    } catch (NullPointerException e) {
      // expected
    }

    try {
      new ReportLogBlockFilter("name", "", null, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new ReportLogBlockFilter("", "match", null, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    new ReportLogBlockFilter("name", "match", null, metrics);
  }

  /** tests that creating a new ReportLogBlockFilter with v2 predicates works as expected */
  @Test
  public void testReportLogBlockFilters_CreateV2PredicatesExists() {
    try {
      new ReportLogBlockFilter("name", null, x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new ReportLogBlockFilter(null, "match", x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new ReportLogBlockFilter("name", "", x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new ReportLogBlockFilter("", "match", x -> false, metrics);
    } catch (IllegalArgumentException e) {
      // expected
    }

    new ReportLogBlockFilter("name", "match", x -> false, metrics);
    new ReportLogBlockFilter(null, null, x -> false, metrics);
  }

  /** tests that new ReportLogBlockFilters blocks all logs it should */
  @Test
  public void testReportLogBlockFilters_testNotAllowed() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("abc")
            .setMessage("123")
            .setAnnotations(existingAnnotations)
            .build();

    ReportLogBlockFilter filter1 =
        new ReportLogBlockFilter("message", "^[0-9]+", x -> true, metrics);
    ReportLogBlockFilter filter2 =
        new ReportLogBlockFilter("sourceName", "^[a-z]+", x -> true, metrics);
    ReportLogBlockFilter filter3 = new ReportLogBlockFilter("foo", "bar", x -> true, metrics);
    ReportLogBlockFilter filter4 = new ReportLogBlockFilter(null, null, x -> true, metrics);

    // Act
    boolean isAllowed1 = filter1.test(log);
    boolean isAllowed2 = filter2.test(log);
    boolean isAllowed3 = filter3.test(log);
    boolean isAllowed4 = filter4.test(log);

    // Assert
    assertFalse(isAllowed1);
    assertFalse(isAllowed2);
    assertFalse(isAllowed3);
    assertFalse(isAllowed4);
  }

  /** tests that new ReportLogBlockFilters allows all logs it should */
  @Test
  public void testReportLogBlockFilters_testAllowed() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("abc")
            .setMessage("123")
            .setAnnotations(existingAnnotations)
            .build();

    ReportLogBlockFilter filter1 = new ReportLogBlockFilter(null, null, x -> false, metrics);
    ReportLogBlockFilter filter2 = new ReportLogBlockFilter("sourceName", "^[0-9]+", null, metrics);
    ReportLogBlockFilter filter3 = new ReportLogBlockFilter("message", "^[a-z]+", null, metrics);
    ReportLogBlockFilter filter4 = new ReportLogBlockFilter("foo", "bar2", null, metrics);

    // Act
    boolean isAllowed1 = filter1.test(log);
    boolean isAllowed2 = filter2.test(log);
    boolean isAllowed3 = filter3.test(log);
    boolean isAllowed4 = filter4.test(log);

    // Assert
    assertTrue(isAllowed1);
    assertTrue(isAllowed2);
    assertTrue(isAllowed3);
    assertTrue(isAllowed4);
  }

  /** tests that ReportLogDropTagTransformer successfully drops tags */
  @Test
  public void testReportLogDropTagTransformer() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
    existingAnnotations.add(Annotation.newBuilder().setKey("k1").setValue("v1").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("abc")
            .setMessage("123")
            .setAnnotations(existingAnnotations)
            .build();

    Map<String, String> allowedTags = new HashMap<>();
    allowedTags.put("k1", "v1");
    ReportLogDropTagTransformer rule =
        new ReportLogDropTagTransformer("foo", "bar1", null, metrics);
    ReportLogDropTagTransformer rule2 = new ReportLogDropTagTransformer("k1", null, null, metrics);

    // Act
    rule.apply(log);
    rule2.apply(log);

    // Assert
    assertEquals(log.getAnnotations().size(), 1);
    assertTrue(log.getAnnotations().contains(new Annotation("foo", "bar")));
  }

  /** tests that ReportLogExtractTagTransformer successfully extract tags */
  @Test
  public void testReportLogExtractTagTransformer() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("abc")
            .setMessage("123abc123")
            .setAnnotations(existingAnnotations)
            .build();

    ReportLogExtractTagTransformer messageRule =
        new ReportLogExtractTagTransformer(
            "t1",
            "message",
            "([0-9]+)([a-z]+)([0-9]+)",
            "$2",
            "$1$3",
            "([0-9a-z]+)",
            null,
            metrics);

    ReportLogExtractTagTransformer sourceRule =
        new ReportLogExtractTagTransformer(
            "t2", "sourceName", "(a)(b)(c)", "$2$3", "$1$3", "^[a-z]+", null, metrics);

    ReportLogExtractTagTransformer annotationRule =
        new ReportLogExtractTagTransformer(
            "t3", "foo", "(b)(ar)", "$1", "$2", "^[a-z]+", null, metrics);

    ReportLogExtractTagTransformer invalidPatternMatch =
        new ReportLogExtractTagTransformer(
            "t3", "foo", "asdf", "$1", "$2", "badpattern", null, metrics);

    ReportLogExtractTagTransformer invalidPatternSearch =
        new ReportLogExtractTagTransformer(
            "t3", "foo", "badpattern", "$1", "$2", "^[a-z]+", null, metrics);

    // test message extraction
    // Act + Assert
    messageRule.apply(log);
    assertEquals(2, log.getAnnotations().size());
    assertTrue(log.getAnnotations().contains(new Annotation("t1", "abc")));
    assertEquals("123123", log.getMessage());

    // test host extraction
    // Act + Assert
    sourceRule.apply(log);
    assertEquals(3, log.getAnnotations().size());
    assertTrue(log.getAnnotations().contains(new Annotation("t2", "bc")));
    assertEquals("ac", log.getHost());

    // test annotation extraction
    // Act + Assert
    annotationRule.apply(log);
    assertEquals(4, log.getAnnotations().size());
    assertTrue(log.getAnnotations().contains(new Annotation("t3", "b")));
    assertTrue(log.getAnnotations().contains(new Annotation("foo", "ar")));

    // test invalid pattern match
    // Act + Assert
    invalidPatternMatch.apply(log);
    assertEquals(4, log.getAnnotations().size());

    // test invalid pattern search
    // Act + Assert
    invalidPatternSearch.apply(log);
    assertEquals(4, log.getAnnotations().size());
  }

  /** tests that ReportLogForceLowerCaseTransformer successfully sets logs to lower case */
  @Test
  public void testReportLogForceLowerCaseTransformer() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("baR").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("Abc")
            .setMessage("dEf")
            .setAnnotations(existingAnnotations)
            .build();

    ReportLogForceLowercaseTransformer messageRule =
        new ReportLogForceLowercaseTransformer("message", ".*", null, metrics);

    ReportLogForceLowercaseTransformer sourceRule =
        new ReportLogForceLowercaseTransformer("sourceName", ".*", null, metrics);

    ReportLogForceLowercaseTransformer annotationRule =
        new ReportLogForceLowercaseTransformer("foo", ".*", null, metrics);

    ReportLogForceLowercaseTransformer messagePatternMismatchRule =
        new ReportLogForceLowercaseTransformer("message", "doesNotMatch", null, metrics);

    ReportLogForceLowercaseTransformer sourcePatternMismatchRule =
        new ReportLogForceLowercaseTransformer("sourceName", "doesNotMatch", null, metrics);

    // test message pattern mismatch
    // Act + Assert
    messagePatternMismatchRule.apply(log);
    assertEquals("dEf", log.getMessage());

    // test host pattern mismatch
    // Act + Assert
    sourcePatternMismatchRule.apply(log);
    assertEquals("Abc", log.getHost());

    // test message to lower case
    // Act + Assert
    messageRule.apply(log);
    assertEquals(1, log.getAnnotations().size());
    assertEquals("def", log.getMessage());

    // test host to lower case
    // Act + Assert
    sourceRule.apply(log);
    assertEquals(1, log.getAnnotations().size());
    assertEquals("abc", log.getHost());

    // test annotation to lower case
    // Act + Assert
    annotationRule.apply(log);
    assertEquals(1, log.getAnnotations().size());
    assertTrue(log.getAnnotations().contains(new Annotation("foo", "bar")));
  }

  /** tests that ReportLogLimitLengthTransformer successfully limits log length */
  @Test
  public void testReportLogLimitLengthTransformer() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("abc")
            .setMessage("123456")
            .setAnnotations(existingAnnotations)
            .build();

    ReportLogLimitLengthTransformer messageRule =
        new ReportLogLimitLengthTransformer(
            "message", 5, TRUNCATE_WITH_ELLIPSIS, null, null, metrics);

    ReportLogLimitLengthTransformer sourceRule =
        new ReportLogLimitLengthTransformer("sourceName", 2, TRUNCATE, null, null, metrics);

    ReportLogLimitLengthTransformer annotationRule =
        new ReportLogLimitLengthTransformer("foo", 2, TRUNCATE, ".*", null, metrics);

    ReportLogLimitLengthTransformer messagePatternMismatchRule =
        new ReportLogLimitLengthTransformer("message", 2, TRUNCATE, "doesNotMatch", null, metrics);

    ReportLogLimitLengthTransformer sourcePatternMismatchRule =
        new ReportLogLimitLengthTransformer(
            "sourceName", 2, TRUNCATE, "doesNotMatch", null, metrics);

    // test message pattern mismatch
    // Act + Assert
    messagePatternMismatchRule.apply(log);
    assertEquals("123456", log.getMessage());

    // test host pattern mismatch
    // Act + Assert
    sourcePatternMismatchRule.apply(log);
    assertEquals("abc", log.getHost());

    // test message length limit
    // Act + Assert
    messageRule.apply(log);
    assertEquals("12...", log.getMessage());

    // test host length limit
    // Act + Assert
    sourceRule.apply(log);
    assertEquals("ab", log.getHost());

    // test annotation length limit
    // Act + Assert
    annotationRule.apply(log);
    assertEquals(1, log.getAnnotations().size());
    assertTrue(log.getAnnotations().contains(new Annotation("foo", "ba")));
  }

  /** tests that ReportLogRenameTagTransformer successfully renames tags */
  @Test
  public void testReportLogRenameTagTransformer() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("abc")
            .setMessage("123456")
            .setAnnotations(existingAnnotations)
            .build();

    ReportLogRenameTagTransformer annotationRule =
        new ReportLogRenameTagTransformer("foo", "foo2", ".*", null, metrics);

    ReportLogRenameTagTransformer PatternMismatchRule =
        new ReportLogRenameTagTransformer("foo", "foo3", "doesNotMatch", null, metrics);

    // test pattern mismatch
    // Act + Assert
    PatternMismatchRule.apply(log);
    assertEquals(1, log.getAnnotations().size());
    assertTrue(log.getAnnotations().contains(new Annotation("foo", "bar")));

    // test annotation rename
    // Act + Assert
    annotationRule.apply(log);
    assertEquals(1, log.getAnnotations().size());
    assertTrue(log.getAnnotations().contains(new Annotation("foo2", "bar")));
  }

  /** tests that ReportLogReplaceRegexTransformer successfully replaces using Regex */
  @Test
  public void testReportLogReplaceRegexTransformer() {
    // Arrange
    List<Annotation> existingAnnotations = new ArrayList<>();
    existingAnnotations.add(Annotation.newBuilder().setKey("foo").setValue("bar").build());
    ReportLog log =
        ReportLog.newBuilder()
            .setTimestamp(0)
            .setHost("abc")
            .setMessage("123456")
            .setAnnotations(existingAnnotations)
            .build();

    ReportLogReplaceRegexTransformer messageRule =
        new ReportLogReplaceRegexTransformer("message", "12", "ab", null, 5, null, metrics);

    ReportLogReplaceRegexTransformer sourceRule =
        new ReportLogReplaceRegexTransformer("sourceName", "ab", "12", null, 5, null, metrics);

    ReportLogReplaceRegexTransformer annotationRule =
        new ReportLogReplaceRegexTransformer("foo", "bar", "ouch", ".*", 5, null, metrics);

    ReportLogReplaceRegexTransformer messagePatternMismatchRule =
        new ReportLogReplaceRegexTransformer(
            "message", "123", "abc", "doesNotMatch", 5, null, metrics);

    ReportLogReplaceRegexTransformer sourcePatternMismatchRule =
        new ReportLogReplaceRegexTransformer(
            "sourceName", "abc", "123", "doesNotMatch", 5, null, metrics);

    // test message pattern mismatch
    // Act + Assert
    messagePatternMismatchRule.apply(log);
    assertEquals("123456", log.getMessage());

    // test host pattern mismatch
    // Act + Assert
    sourcePatternMismatchRule.apply(log);
    assertEquals("abc", log.getHost());

    // test message regex replace
    // Act + Assert
    messageRule.apply(log);
    assertEquals("ab3456", log.getMessage());

    // test host regex replace
    // Act + Assert
    sourceRule.apply(log);
    assertEquals("12c", log.getHost());

    // test annotation regex replace
    // Act + Assert
    annotationRule.apply(log);
    assertEquals(1, log.getAnnotations().size());
    assertTrue(log.getAnnotations().contains(new Annotation("foo", "ouch")));
  }
}
