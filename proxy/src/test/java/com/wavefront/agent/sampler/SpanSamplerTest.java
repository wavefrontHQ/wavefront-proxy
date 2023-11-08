package com.wavefront.agent.sampler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.wavefront.api.agent.SpanSamplingPolicy;
import com.wavefront.data.AnnotationUtils;
import com.wavefront.sdk.entities.tracing.sampling.DurationSampler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.Span;

/** @author Han Zhang (zhanghan@vmware.com) */
public class SpanSamplerTest {
  @Test
  public void testSample() {
    long startTime = System.currentTimeMillis();
    String traceId = UUID.randomUUID().toString();
    Span spanToAllow =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(6)
            .setName("testSpanName")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(traceId)
            .build();
    Span spanToDiscard =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(4)
            .setName("testSpanName")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(traceId)
            .build();
    SpanSampler sampler = new SpanSampler(new DurationSampler(5), () -> null, null);
    assertTrue(sampler.sample(spanToAllow));
    assertFalse(sampler.sample(spanToDiscard));

    Counter discarded =
        Metrics.newCounter(new MetricName("SpanSamplerTest", "testSample", "discarded"));
    assertTrue(sampler.sample(spanToAllow, discarded));
    assertEquals(0, discarded.count());
    assertFalse(sampler.sample(spanToDiscard, discarded));
    assertEquals(1, discarded.count());
  }

  @Test
  public void testAlwaysSampleDebug() {
    long startTime = System.currentTimeMillis();
    String traceId = UUID.randomUUID().toString();
    Span span =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(4)
            .setName("testSpanName")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(traceId)
            .setAnnotations(ImmutableList.of(new Annotation("debug", "true")))
            .build();
    SpanSampler sampler = new SpanSampler(new DurationSampler(5), () -> null, null);
    assertTrue(sampler.sample(span));
  }

  @Test
  public void testMultipleSpanSamplingPolicies() {
    long startTime = System.currentTimeMillis();
    String traceId = UUID.randomUUID().toString();
    Span spanToAllow =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(4)
            .setName("testSpanName")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(traceId)
            .build();
    Span spanToAllowWithDebugTag =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(4)
            .setName("testSpanName")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(traceId)
            .setAnnotations(ImmutableList.of(new Annotation("debug", "true")))
            .build();
    Span spanToDiscard =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(4)
            .setName("testSpanName")
            .setSource("source")
            .setSpanId("testspanid")
            .setTraceId(traceId)
            .build();
    List<SpanSamplingPolicy> activeSpanSamplingPolicies =
        ImmutableList.of(
            new SpanSamplingPolicy("SpanNamePolicy", "{{spanName}}='testSpanName'", 0),
            new SpanSamplingPolicy("SpanSourcePolicy", "{{sourceName}}='testsource'", 100));

    SpanSampler sampler =
        new SpanSampler(new DurationSampler(5), () -> activeSpanSamplingPolicies, null);
    assertTrue(sampler.sample(spanToAllow));
    assertEquals(
        "SpanSourcePolicy",
        AnnotationUtils.getValue(spanToAllow.getAnnotations(), "_sampledByPolicy"));
    assertTrue(sampler.sample(spanToAllowWithDebugTag));
    assertNull(
        AnnotationUtils.getValue(spanToAllowWithDebugTag.getAnnotations(), "_sampledByPolicy"));
    assertFalse(sampler.sample(spanToDiscard));
    assertTrue(spanToDiscard.getAnnotations().isEmpty());
  }

  @Test
  public void testSpanSamplingPolicySamplingPercent() {
    long startTime = System.currentTimeMillis();
    Span span =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(4)
            .setName("testSpanName")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(UUID.randomUUID().toString())
            .build();
    List<SpanSamplingPolicy> activeSpanSamplingPolicies = new ArrayList<>();
    activeSpanSamplingPolicies.add(
        new SpanSamplingPolicy("SpanNamePolicy", "{{spanName}}='testSpanName'", 50));
    SpanSampler sampler =
        new SpanSampler(new DurationSampler(5), () -> activeSpanSamplingPolicies, null);
    int sampledSpans = 0;
    for (int i = 0; i < 1000; i++) {
      if (sampler.sample(Span.newBuilder(span).setTraceId(UUID.randomUUID().toString()).build())) {
        sampledSpans++;
      }
    }
    assertTrue(sampledSpans < 1000 && sampledSpans > 0);
    activeSpanSamplingPolicies.clear();
    activeSpanSamplingPolicies.add(
        new SpanSamplingPolicy("SpanNamePolicy", "{{spanName" + "}}='testSpanName'", 100));
    sampledSpans = 0;
    for (int i = 0; i < 1000; i++) {
      if (sampler.sample(Span.newBuilder(span).setTraceId(UUID.randomUUID().toString()).build())) {
        sampledSpans++;
      }
    }
    assertEquals(1000, sampledSpans);
    activeSpanSamplingPolicies.clear();
    activeSpanSamplingPolicies.add(
        new SpanSamplingPolicy("SpanNamePolicy", "{{spanName" + "}}='testSpanName'", 0));
    sampledSpans = 0;
    for (int i = 0; i < 1000; i++) {
      if (sampler.sample(Span.newBuilder(span).setTraceId(UUID.randomUUID().toString()).build())) {
        sampledSpans++;
      }
    }
    assertEquals(0, sampledSpans);
  }
}
