package com.wavefront.data;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.ingester.GraphiteDecoder;
import com.wavefront.ingester.HistogramDecoder;
import com.wavefront.ingester.SpanDecoder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import wavefront.report.Annotation;
import wavefront.report.Histogram;
import wavefront.report.HistogramType;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author vasily@wavefront.com
 */
public class ValidationTest {

  private ValidationConfiguration config;
  private GraphiteDecoder decoder;

  @Before
  public void testSetup() {
    this.decoder = new GraphiteDecoder(ImmutableList.of());
    this.config = new ValidationConfiguration().
        setMetricLengthLimit(15).
        setHostLengthLimit(10).
        setHistogramLengthLimit(10).
        setSpanLengthLimit(20).
        setAnnotationsCountLimit(4).
        setAnnotationsKeyLengthLimit(5).
        setAnnotationsValueLengthLimit(10).
        setSpanAnnotationsCountLimit(3).
        setSpanAnnotationsKeyLengthLimit(6).
        setSpanAnnotationsValueLengthLimit(15);
  }

  @Test
  public void testPointIllegalChars() {
    String input = "metric1";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good.metric2";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good-metric3";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good_metric4";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good,metric5";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "good/metric6";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // first character can no longer be ~
    input = "~good.metric7";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // first character can be ∆ (\u2206)
    input = "∆delta.metric8";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // first character can be Δ (\u0394)
    input = "Δdelta.metric9";
    Assert.assertTrue(Validation.charactersAreValid(input));

    // non-first character cannot be ~
    input = "~good.~metric";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // non-first character cannot be ∆ (\u2206)
    input = "∆delta.∆metric";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // non-first character cannot be Δ (\u0394)
    input = "∆delta.Δmetric";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // cannot end in ~
    input = "good.metric.~";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // cannot end in ∆ (\u2206)
    input = "delta.metric.∆";
    Assert.assertFalse(Validation.charactersAreValid(input));

    // cannot end in Δ (\u0394)
    input = "delta.metric.Δ";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "abcdefghijklmnopqrstuvwxyz.0123456789,/_-ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    Assert.assertTrue(Validation.charactersAreValid(input));

    input = "abcdefghijklmnopqrstuvwxyz.0123456789,/_-ABCDEFGHIJKLMNOPQRSTUVWXYZ~";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as;df";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as:df";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as df";
    Assert.assertFalse(Validation.charactersAreValid(input));

    input = "as'df";
    Assert.assertFalse(Validation.charactersAreValid(input));
  }

  @Test
  public void testPointAnnotationKeyValidation() {
    Map<String, String> goodMap = new HashMap<String, String>();
    goodMap.put("key", "value");

    Map<String, String> badMap = new HashMap<String, String>();
    badMap.put("k:ey", "value");

    ReportPoint rp = new ReportPoint("some metric", System.currentTimeMillis(), 10L, "host", "table",
        goodMap);
    Assert.assertTrue(Validation.annotationKeysAreValid(rp));

    rp.setAnnotations(badMap);
    Assert.assertFalse(Validation.annotationKeysAreValid(rp));
  }

  @Test
  public void testValidationConfig() {
    ReportPoint point = getValidPoint();
    Validation.validatePoint(point, config);

    ReportPoint histogram = getValidHistogram();
    Validation.validatePoint(histogram, config);

    Span span = getValidSpan();
    Validation.validateSpan(span, config);
  }

  @Test
  public void testInvalidPointsWithValidationConfig() {
    ReportPoint point = getValidPoint();
    Validation.validatePoint(point, config);

    // metric has invalid characters: WF-400
    point.setMetric("metric78@901234");
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-400"));
    }

    // point tag key has invalid characters: WF-401
    point = getValidPoint();
    point.getAnnotations().remove("tagk4");
    point.getAnnotations().put("tag!4", "value");
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-401"));
    }

    // string values are not allowed: WF-403
    point = getValidPoint();
    point.setValue("stringValue");
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-403"));
    }

    // delta metrics can't be non-positive: WF-404
    point = getValidPoint();
    point.setMetric("∆delta");
    point.setValue(1.0d);
    Validation.validatePoint(point, config);
    point.setValue(1L);
    Validation.validatePoint(point, config);
    point.setValue(-0.1d);
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-404"));
    }
    point.setValue(0.0d);
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-404"));
    }
    point.setValue(-1L);
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-404"));
    }

    // empty histogram: WF-405
    point = getValidHistogram();
    Validation.validatePoint(point, config);

    ((Histogram) point.getValue()).setCounts(ImmutableList.of(0, 0, 0));
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-405"));
    }
    point = getValidHistogram();
    ((Histogram) point.getValue()).setBins(ImmutableList.of());
    ((Histogram) point.getValue()).setCounts(ImmutableList.of());
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-405"));
    }

    // missing source: WF-406
    point = getValidPoint();
    point.setHost("");
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-406"));
    }

    // source name too long: WF-407
    point = getValidPoint();
    point.setHost("host567890");
    Validation.validatePoint(point, config);
    point = getValidPoint();
    point.setHost("host5678901");
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-407"));
    }

    // metric too long: WF-408
    point = getValidPoint();
    point.setMetric("metric789012345");
    Validation.validatePoint(point, config);
    point = getValidPoint();
    point.setMetric("metric7890123456");
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-408"));
    }

    // histogram name too long: WF-409
    point = getValidHistogram();
    point.setMetric("metric7890");
    Validation.validatePoint(point, config);
    point = getValidHistogram();
    point.setMetric("metric78901");
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-409"));
    }

    // too many point tags: WF-410
    point = getValidPoint();
    point.getAnnotations().put("newtag", "newtagV");
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-410"));
    }

    // point tag (key+value) too long: WF-411
    point = getValidPoint();
    point.getAnnotations().remove("tagk4");
    point.getAnnotations().put(Strings.repeat("k", 100), Strings.repeat("v", 154));
    ValidationConfiguration tagConfig = new ValidationConfiguration().
        setAnnotationsKeyLengthLimit(255).
        setAnnotationsValueLengthLimit(255);
    Validation.validatePoint(point, tagConfig);
    point.getAnnotations().put(Strings.repeat("k", 100), Strings.repeat("v", 155));
    try {
      Validation.validatePoint(point, tagConfig);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-411"));
    }

    // point tag key too long: WF-412
    point = getValidPoint();
    point.getAnnotations().remove("tagk4");
    point.getAnnotations().put("tagk44", "v");
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-412"));
    }

    // point tag value too long: WF-413
    point = getValidPoint();
    point.getAnnotations().put("tagk4", "value67890");
    Validation.validatePoint(point, config);
    point = getValidPoint();
    point.getAnnotations().put("tagk4", "value678901");
    try {
      Validation.validatePoint(point, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-413"));
    }
  }

  @Test
  public void testInvalidSpansWithValidationConfig() {
    Span span;

    // span name has invalid characters: WF-415
    span = getValidSpan();
    span.setName("span~name");
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-415"));
    }

    // span annotation key has invalid characters: WF-416
    span = getValidSpan();
    span.getAnnotations().add(new Annotation("$key", "v"));
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-416"));
    }

    // span missing source: WF-426
    span = getValidSpan();
    span.setSource("");
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-426"));
    }

    // span source name too long: WF-427
    span = getValidSpan();
    span.setSource("source7890");
    Validation.validateSpan(span, config);
    span = getValidSpan();
    span.setSource("source78901");
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-427"));
    }

    // span name too long: WF-428
    span = getValidSpan();
    span.setName("spanName901234567890");
    Validation.validateSpan(span, config);
    span = getValidSpan();
    span.setName("spanName9012345678901");
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-428"));
    }

    // span has too many annotations: WF-430
    span = getValidSpan();
    span.getAnnotations().add(new Annotation("k1", "v1"));
    span.getAnnotations().add(new Annotation("k2", "v2"));
    Validation.validateSpan(span, config);
    span.getAnnotations().add(new Annotation("k3", "v3"));
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-430"));
    }

    // span annotation (key+value) too long: WF-431
    span = getValidSpan();
    span.getAnnotations().add(new Annotation(Strings.repeat("k", 100), Strings.repeat("v", 154)));
    ValidationConfiguration tagConfig = new ValidationConfiguration().
        setSpanAnnotationsKeyLengthLimit(255).
        setSpanAnnotationsValueLengthLimit(255);
    Validation.validateSpan(span, tagConfig);
    span = getValidSpan();
    span.getAnnotations().add(new Annotation(Strings.repeat("k", 100), Strings.repeat("v", 155)));
    try {
      Validation.validateSpan(span, tagConfig);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-431"));
    }

    // span annotation key too long: WF-432
    span = getValidSpan();
    span.getAnnotations().add(new Annotation("k23456", "v1"));
    Validation.validateSpan(span, config);
    span = getValidSpan();
    span.getAnnotations().add(new Annotation("k234567", "v1"));
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-432"));
    }

    // span annotation value too long: WF-433
    span = getValidSpan();
    span.getAnnotations().add(new Annotation("k", "v23456789012345"));
    Validation.validateSpan(span, config);
    span = getValidSpan();
    span.getAnnotations().add(new Annotation("k", "v234567890123456"));
    try {
      Validation.validateSpan(span, config);
      fail();
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("WF-433"));
    }
  }

  @Test
  public void testValidHistogram() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("!M 1533849540 #1 0.0 #2 1.0 #3 3.0 TestMetric source=Test key=value", out, "dummy");
    Validation.validatePoint(out.get(0), "test", Validation.Level.NUMERIC_ONLY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyHistogramThrows() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("!M 1533849540 #0 0.0 #0 1.0 #0 3.0 TestMetric source=Test key=value", out, "dummy");
    Validation.validatePoint(out.get(0), "test", Validation.Level.NUMERIC_ONLY);
    Assert.fail("Empty Histogram should fail validation!");
  }

  private ReportPoint getValidPoint() {
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("metric789012345 1 source=source7890 tagk1=tagv1 tagk2=tagv2 tagk3=tagv3 tagk4=tagv4",
        out, "dummy");
    return out.get(0);
  }

  private ReportPoint getValidHistogram() {
    HistogramDecoder decoder = new HistogramDecoder();
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("!M 1533849540 #1 0.0 #2 1.0 #3 3.0 TestMetric source=Test key=value", out, "dummy");
    return out.get(0);
  }

  private Span getValidSpan() {
    List<Span> spanOut = new ArrayList<>();
    SpanDecoder spanDecoder = new SpanDecoder("default");
    spanDecoder.decode("testSpanName source=spanSource spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
            "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 tagkey=tagvalue1 1532012145123456 1532012146234567 ",
        spanOut);
    return spanOut.get(0);
  }

}
