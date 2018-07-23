package com.wavefront.data;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import wavefront.report.ReportPoint;

/**
 * @author vasily@wavefront.com
 */
public class ValidationTest {

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
    Assert.assertFalse(Validation.charactersAreValid(input));

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

}
