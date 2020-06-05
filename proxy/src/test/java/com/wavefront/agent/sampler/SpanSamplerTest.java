package com.wavefront.agent.sampler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.wavefront.sdk.entities.tracing.sampling.DurationSampler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.Span;

import java.util.UUID;

/**
 * @author Han Zhang (zhanghan@vmware.com)
 */
public class SpanSamplerTest {
  @Test
  public void testSample() {
    long startTime = System.currentTimeMillis();
    String traceId = UUID.randomUUID().toString();
    Span spanToAllow = Span.newBuilder().
        setCustomer("dummy").
        setStartMillis(startTime).
        setDuration(6).
        setName("testSpanName").
        setSource("testsource").
        setSpanId("testspanid").
        setTraceId(traceId).
        build();
    Span spanToDiscard = Span.newBuilder().
        setCustomer("dummy").
        setStartMillis(startTime).
        setDuration(4).
        setName("testSpanName").
        setSource("testsource").
        setSpanId("testspanid").
        setTraceId(traceId).
        build();
    SpanSampler sampler = new SpanSampler(new DurationSampler(5), true);
    assertTrue(sampler.sample(spanToAllow));
    assertFalse(sampler.sample(spanToDiscard));

    Counter discarded = Metrics.newCounter(new MetricName("SpanSamplerTest", "testSample",
        "discarded"));
    assertTrue(sampler.sample(spanToAllow, discarded));
    assertEquals(0, discarded.count());
    assertFalse(sampler.sample(spanToDiscard, discarded));
    assertEquals(1, discarded.count());
  }

  @Test
  public void testAlwaysSampleErrors() {
    long startTime = System.currentTimeMillis();
    String traceId = UUID.randomUUID().toString();
    Span span = Span.newBuilder().
        setCustomer("dummy").
        setStartMillis(startTime).
        setDuration(4).
        setName("testSpanName").
        setSource("testsource").
        setSpanId("testspanid").
        setTraceId(traceId).
        setAnnotations(ImmutableList.of(new Annotation("error", "true"))).
        build();
    SpanSampler sampler = new SpanSampler(new DurationSampler(5), true);
    assertTrue(sampler.sample(span));
    sampler = new SpanSampler(new DurationSampler(5), false);
    assertFalse(sampler.sample(span));
  }
}
