package com.wavefront.agent.sampler;

import static com.wavefront.common.TraceConstants.PARENT_KEY;
import static com.wavefront.sdk.common.Constants.ERROR_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static org.junit.Assert.*;

import java.util.List;
import java.util.UUID;
import org.apache.commons.compress.utils.Lists;
import org.junit.Before;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.Span;

public class EbpfPreferedSamplerTest {

  @Before
  public void reset() {
    EbpfSampler.EdgeStats.totalEdgeCount.set(0);
  }

  @Test
  public void testInsignificant() {
    EbpfSampler sampler = new EbpfSampler(1.0, 100);
    List<Annotation> annotations = Lists.newArrayList();
    annotations.add(Annotation.newBuilder().setKey(PARENT_KEY).setValue("parentId").build());

    Span span1 =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(System.currentTimeMillis())
            .setDuration(4)
            .setName("epbf")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(UUID.randomUUID().toString())
            .setAnnotations(annotations)
            .build();
    for (int i = 0; i < 10; i++) assertTrue(sampler.sample(span1));
  }

  @Test
  public void testSampling() {
    EbpfSampler sampler = new EbpfSampler(1.0, 100);
    List<Annotation> annotations = Lists.newArrayList();
    annotations.add(Annotation.newBuilder().setKey(PARENT_KEY).setValue("parentId").build());

    Span span1 =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(System.currentTimeMillis())
            .setDuration(4)
            .setName("epbf")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(UUID.randomUUID().toString())
            .setAnnotations(annotations)
            .build();

    // sampled when insufficient
    for (int i = 0; i < 99; i++) {
      assertTrue(sampler.sample(span1));
    }

    // 100% due to only one type
    assertFalse(sampler.sample(span1));

    // long duration
    Span longSpan =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(System.currentTimeMillis())
            .setDuration(1000)
            .setName("epbf")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(UUID.randomUUID().toString())
            .setAnnotations(annotations)
            .build();
    sampler.sample(longSpan);

    List<Annotation> arrorAnnotations = Lists.newArrayList(annotations.listIterator());
    arrorAnnotations.add(Annotation.newBuilder().setKey(ERROR_TAG_KEY).setValue("true").build());
    Span errorSpan =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(System.currentTimeMillis())
            .setDuration(5)
            .setName("epbf")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(UUID.randomUUID().toString())
            .setAnnotations(arrorAnnotations)
            .build();
    assertTrue(sampler.sample(errorSpan));

    List<Annotation> annotations2 = Lists.newArrayList();
    annotations2.add(Annotation.newBuilder().setKey(SERVICE_TAG_KEY).setValue("dest2").build());
    annotations2.add(Annotation.newBuilder().setKey(PARENT_KEY).setValue("parentId").build());

    Span span2 =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(System.currentTimeMillis())
            .setDuration(4)
            .setName("epbf")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId("UUID.randomUUID().toString()")
            .setAnnotations(annotations2)
            .build();

    // new edge should be sampled
    assertTrue(sampler.sample(span2));

    // some will not sampled
    boolean someNotSampled = false;
    for (int i = 0; i < 10; i++) {
      if (!sampler.sample(span1)) {
        someNotSampled = true;
        System.out.println(String.format("Not sampled at %d", i));
        break;
      }
    }
    if (!someNotSampled) fail();
  }

  @Test
  public void testMultipleEdges() {
    EbpfSampler sampler = new EbpfSampler(1.0, 100);
    List<Annotation> annotations = Lists.newArrayList();
    annotations.add(Annotation.newBuilder().setKey(PARENT_KEY).setValue("parentId").build());

    Span span1 =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(System.currentTimeMillis())
            .setDuration(4)
            .setName("epbf")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(UUID.randomUUID().toString())
            .setAnnotations(annotations)
            .build();

    // long duration
    List<Annotation> annotations2 = Lists.newArrayList();
    annotations2.add(Annotation.newBuilder().setKey(SERVICE_TAG_KEY).setValue("dest2").build());
    annotations2.add(Annotation.newBuilder().setKey(PARENT_KEY).setValue("parentId").build());

    Span span2 =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(System.currentTimeMillis())
            .setDuration(1000)
            .setName("epbf")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(UUID.randomUUID().toString())
            .setAnnotations(annotations2)
            .build();
    // sampled when insufficient
    for (int i = 0; i < 66; i++) {
      assertTrue(sampler.sample(span1));
      if (i % 2 == 0) assertTrue(sampler.sample(span2));
    }

    boolean someNotSampled = false;
    for (int i = 0; i < 15; i++) {
      if (!sampler.sample(span1)) {
        someNotSampled = true;
        System.out.println(String.format("Not sampled at %d", i));
        break;
      }
    }
    if (!someNotSampled) fail();

    span2.setDuration(5);
    someNotSampled = false;
    for (int i = 0; i < 40; i++) {
      if (!sampler.sample(span2)) {
        someNotSampled = true;
        System.out.println(String.format("Not sampled at %d", i));
        break;
      }
    }
    if (!someNotSampled) fail();
  }

  @Test
  public void testSamplingFactor() {
    assertEquals(0.09, testEstimatingSamplingFactor(12), 0.000001d);
    assertEquals(0.008, testEstimatingSamplingFactor(1000000), 0.000001d);
  }

  private double testEstimatingSamplingFactor(int totalTypes) {
    List<Annotation> annotations = Lists.newArrayList();
    annotations.add(Annotation.newBuilder().setKey(PARENT_KEY).setValue("parentId").build());

    EbpfSampler sampler = new EbpfSampler(1.0 / totalTypes, 100);
    this.reset();

    for (int type = 0; type < totalTypes; type++) {
      Span span1 =
          Span.newBuilder()
              .setCustomer("dummy")
              .setStartMillis(System.currentTimeMillis())
              .setDuration(4)
              .setName("epbf")
              .setSource("testsource" + type)
              .setSpanId("testspanid")
              .setTraceId(UUID.randomUUID().toString())
              .setAnnotations(annotations)
              .build();
      sampler.sample(span1);
    }
    double factor = sampler.estimateSamplingFactor();
    System.out.println(String.format("total types: %d, samplingPower: %f", totalTypes, factor));

    return factor;
  }

  public void testTypeRatioToSamplingRate() {
    for (int typeRatio = 0; typeRatio < 100; typeRatio++) {
      System.out.println(
          String.format(
              "%d%% %f %f %f %f %f %f %f",
              typeRatio,
              1 - Math.pow(typeRatio / 100.0, 0.5),
              1 - Math.pow(typeRatio / 100.0, 0.4),
              1 - Math.pow(typeRatio / 100.0, 0.3),
              1 - Math.pow(typeRatio / 100.0, 0.2),
              1 - Math.pow(typeRatio / 100.0, 0.1),
              1 - Math.pow(typeRatio / 100.0, 0.01),
              1 - Math.pow(typeRatio / 100.0, 0.005)));
    }
  }
}
