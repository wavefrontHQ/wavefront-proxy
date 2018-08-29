package com.wavefront.ingester;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.function.Function;

import wavefront.report.Annotation;
import wavefront.report.Span;

import static org.junit.Assert.assertEquals;

/**
 * @author vasily@wavefront.com
 */
public class SpanSerializerTest {

  private Function<Span, String> serializer = new SpanSerializer();

  @Test
  public void testSpanToString() {
    Span span = Span.newBuilder()
        .setCustomer("dummy")
        .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
        .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
        .setName("testSpanName")
        .setSource("spanSource")
        .setStartMillis(1532012145123456L)
        .setDuration(1111111L)
        .setAnnotations(ImmutableList.of(new Annotation("tagk1", "tagv1"), new Annotation("tagk2", "tagv2")))
        .build();
    assertEquals("\"testSpanName\" source=\"spanSource\" spanId=\"4217104a-690d-4927-baff-d9aa779414c2\" " +
        "traceId=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" \"tagk1\"=\"tagv1\" \"tagk2\"=\"tagv2\" " +
        "1532012145123456 1111111", serializer.apply(span));
  }

  @Test
  public void testSpanWithQuotesInTagsToString() {
    Span span = Span.newBuilder()
        .setCustomer("dummy")
        .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
        .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
        .setName("testSpanName")
        .setSource("spanSource")
        .setStartMillis(1532012145123456L)
        .setDuration(1111111L)
        .setAnnotations(ImmutableList.of(new Annotation("tagk1", "tag\"v\"1"), new Annotation("tagk2", "\"tagv2")))
        .build();
    assertEquals("\"testSpanName\" source=\"spanSource\" spanId=\"4217104a-690d-4927-baff-d9aa779414c2\" " +
        "traceId=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" \"tagk1\"=\"tag\\\"v\\\"1\" \"tagk2\"=\"\\\"tagv2\" " +
        "1532012145123456 1111111", serializer.apply(span));
  }

  @Test
  public void testSpanWithNullTagsToString() {
    Span span = Span.newBuilder()
        .setCustomer("dummy")
        .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
        .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
        .setName("testSpanName")
        .setSource("spanSource")
        .setStartMillis(1532012145123456L)
        .setDuration(1111111L)
        .build();
    assertEquals("\"testSpanName\" source=\"spanSource\" spanId=\"4217104a-690d-4927-baff-d9aa779414c2\" " +
        "traceId=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" 1532012145123456 1111111", serializer.apply(span));
  }

  @Test
  public void testSpanWithEmptyTagsToString() {
    Span span = Span.newBuilder()
        .setCustomer("dummy")
        .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
        .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
        .setName("testSpanName2")
        .setSource("spanSource")
        .setStartMillis(1532012145123456L)
        .setDuration(1111111L)
        .setAnnotations(ImmutableList.of())
        .build();
    assertEquals("\"testSpanName2\" source=\"spanSource\" spanId=\"4217104a-690d-4927-baff-d9aa779414c2\" " +
        "traceId=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" 1532012145123456 1111111", serializer.apply(span));
  }
}
