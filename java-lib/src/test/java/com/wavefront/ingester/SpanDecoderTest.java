package com.wavefront.ingester;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import wavefront.report.Span;

import static org.junit.Assert.assertEquals;

/**
 * Tests for SpanDecoder
 *
 * @author vasily@wavefront.com
 */
public class SpanDecoderTest {

  // Decoder is supposed to convert all timestamps to this unit. Should we decide to change the timestamp precision
  // in the Span object, this is the only change required to keep unit tests valid.
  private static TimeUnit expectedTimeUnit = TimeUnit.MILLISECONDS;

  private static long startTs = 1532012145123L;
  private static long duration = 1111;

  private SpanDecoder decoder = new SpanDecoder("unitTest");

  @Test(expected = RuntimeException.class)
  public void testSpanWithoutDurationThrows() {
    List<Span> out = new ArrayList<>();
    decoder.decode("testSpanName source=spanSource spanId=spanId traceId=traceId tagkey1=tagvalue1 1532012145123456 ",
        out);
    Assert.fail("Missing duration didn't raise an exception");
  }

  @Test(expected = RuntimeException.class)
  public void testSpanWithExtraTagsThrows() {
    List<Span> out = new ArrayList<>();
    decoder.decode("testSpanName source=spanSource spanId=spanId traceId=traceId tagkey1=tagvalue1 1532012145123456 " +
            "1532012146234567 extraTag=extraValue", out);
    Assert.fail("Extra tags after timestamps didn't raise an exception");
  }

  @Test(expected = RuntimeException.class)
  public void testSpanWithoutSpanIdThrows() {
    List<Span> out = new ArrayList<>();
    decoder.decode("testSpanName source=spanSource traceId=traceId tagkey1=tagvalue1 1532012145123456 " +
        "1532012146234567", out);
    Assert.fail("Missing spanId didn't raise an exception");
  }

  @Test(expected = RuntimeException.class)
  public void testSpanWithoutTraceIdThrows() {
    List<Span> out = new ArrayList<>();
    decoder.decode("testSpanName source=spanSource spanId=spanId tagkey1=tagvalue1 1532012145123456 " +
        "1532012146234567", out);
    Assert.fail("Missing traceId didn't raise an exception");
  }

  @Test
  public void testSpanUsesDefaultSource() {
    List<Span> out = new ArrayList<>();
    decoder.decode("testSpanName spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
            "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 tagkey1=tagvalue1 t2=v2 1532012145123456 1532012146234567 ",
        out);
    assertEquals(1, out.size());
    assertEquals("unitTest", out.get(0).getSource());
  }

  @Test
  public void testSpanWithUnquotedUuids() {
    List<Span> out = new ArrayList<>();
    decoder.decode("testSpanName source=spanSource spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
            "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 tagkey1=tagvalue1 t2=v2 1532012145123456 1532012146234567 ",
        out);
    assertEquals(1, out.size());
    assertEquals("4217104a-690d-4927-baff-d9aa779414c2", out.get(0).getSpanId());
    assertEquals("d5355bf7-fc8d-48d1-b761-75b170f396e0", out.get(0).getTraceId());
  }

  @Test
  public void testSpanWithQuotedUuids() {
    List<Span> out = new ArrayList<>();
    decoder.decode("testSpanName source=spanSource spanId=\"4217104a-690d-4927-baff-d9aa779414c2\" " +
            "traceId=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" tagkey1=tagvalue1 t2=v2 1532012145123456 1532012146234567 ",
        out);
    assertEquals(1, out.size());
    assertEquals("4217104a-690d-4927-baff-d9aa779414c2", out.get(0).getSpanId());
    assertEquals("d5355bf7-fc8d-48d1-b761-75b170f396e0", out.get(0).getTraceId());
  }

  @Test
  public void testSpanWithQuotesInTags() {
    List<Span> out = new ArrayList<>();
    decoder.decode("testSpanName source=spanSource spanId=\"4217104a-690d-4927-baff-d9aa779414c2\" " +
            "traceId=\"traceid\" tagkey1=\"tag\\\"value\\\"1\" t2=v2 1532012145123456 1532012146234567 ",
        out);
    assertEquals(1, out.size());
    assertEquals(2, out.get(0).getAnnotations().size());
    assertEquals("tagkey1", out.get(0).getAnnotations().get(0).getKey());
    assertEquals("tag\"value\"1", out.get(0).getAnnotations().get(0).getValue());
  }

  @Test
  public void testSpanWithRepeatableAnnotations() {
    List<Span> out = new ArrayList<>();
    decoder.decode("testSpanName source=spanSource spanId=spanid traceId=traceid parent=parentid1 tag1=v1 " +
            "parent=parentid2 1532012145123456 1532012146234567 ",
        out);
    assertEquals(1, out.size());
    assertEquals(3, out.get(0).getAnnotations().size());
    assertEquals("parent", out.get(0).getAnnotations().get(0).getKey());
    assertEquals("parentid1", out.get(0).getAnnotations().get(0).getValue());
    assertEquals("parent", out.get(0).getAnnotations().get(2).getKey());
    assertEquals("parentid2", out.get(0).getAnnotations().get(2).getValue());
  }

  @Test
  public void testBasicSpanParse() {
    List<Span> out = new ArrayList<>();
    decoder.decode("testSpanName source=spanSource spanId=4217104a-690d-4927-baff-d9aa779414c2 " +
            "traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 tagkey1=tagvalue1 " +
            "t2=v2 1532012145123 1532012146234", out);
    assertEquals(1, out.size());
    assertEquals("testSpanName", out.get(0).getName());
    assertEquals("spanSource", out.get(0).getSource());
    assertEquals("4217104a-690d-4927-baff-d9aa779414c2", out.get(0).getSpanId());
    assertEquals("d5355bf7-fc8d-48d1-b761-75b170f396e0", out.get(0).getTraceId());
    assertEquals(2, out.get(0).getAnnotations().size());
    assertEquals("tagkey1", out.get(0).getAnnotations().get(0).getKey());
    assertEquals("tagvalue1", out.get(0).getAnnotations().get(0).getValue());
    assertEquals("t2", out.get(0).getAnnotations().get(1).getKey());
    assertEquals("v2", out.get(0).getAnnotations().get(1).getValue());
    assertEquals(expectedTimeUnit.convert(startTs, TimeUnit.MILLISECONDS), 
        (long) out.get(0).getStartMillis());
    assertEquals(expectedTimeUnit.convert(duration, TimeUnit.MILLISECONDS), 
        (long) out.get(0).getDuration());

    out.clear();
    // test that annotations order doesn't matter
    decoder.decode("testSpanName tagkey1=tagvalue1 traceId=d5355bf7-fc8d-48d1-b761-75b170f396e0 " +
            "spanId=4217104a-690d-4927-baff-d9aa779414c2 source=spanSource " +
            "t2=v2 1532012145123 1532012146234 ", out);
    assertEquals(1, out.size());
    assertEquals("testSpanName", out.get(0).getName());
    assertEquals("spanSource", out.get(0).getSource());
    assertEquals("4217104a-690d-4927-baff-d9aa779414c2", out.get(0).getSpanId());
    assertEquals("d5355bf7-fc8d-48d1-b761-75b170f396e0", out.get(0).getTraceId());
    assertEquals(2, out.get(0).getAnnotations().size());
    assertEquals("tagkey1", out.get(0).getAnnotations().get(0).getKey());
    assertEquals("tagvalue1", out.get(0).getAnnotations().get(0).getValue());
    assertEquals("t2", out.get(0).getAnnotations().get(1).getKey());
    assertEquals("v2", out.get(0).getAnnotations().get(1).getValue());
    assertEquals(expectedTimeUnit.convert(startTs, TimeUnit.MILLISECONDS), (long) out.get(0).getStartMillis());
    assertEquals(expectedTimeUnit.convert(duration, TimeUnit.MILLISECONDS), (long) out.get(0).getDuration());

  }

  @Test
  public void testQuotedSpanParse() {
    List<Span> out = new ArrayList<>();
    decoder.decode("\"testSpanName\" \"source\"=\"spanSource\" \"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" " +
            "\"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" \"tagkey1\"=\"tagvalue1\" \"t2\"=\"v2\" " +
            "1532012145123 1532012146234", out);
    assertEquals(1, out.size());
    assertEquals("testSpanName", out.get(0).getName());
    assertEquals("spanSource", out.get(0).getSource());
    assertEquals("4217104a-690d-4927-baff-d9aa779414c2", out.get(0).getSpanId());
    assertEquals("d5355bf7-fc8d-48d1-b761-75b170f396e0", out.get(0).getTraceId());
    assertEquals(2, out.get(0).getAnnotations().size());
    assertEquals("tagkey1", out.get(0).getAnnotations().get(0).getKey());
    assertEquals("tagvalue1", out.get(0).getAnnotations().get(0).getValue());
    assertEquals("t2", out.get(0).getAnnotations().get(1).getKey());
    assertEquals("v2", out.get(0).getAnnotations().get(1).getValue());
    assertEquals(expectedTimeUnit.convert(startTs, TimeUnit.MILLISECONDS), (long) out.get(0).getStartMillis());
    assertEquals(expectedTimeUnit.convert(duration, TimeUnit.MILLISECONDS), (long) out.get(0).getDuration());
  }

  @Test
  public void testSpanTimestampFormats() {
    List<Span> out = new ArrayList<>();
    // nanoseconds with end_ts
    decoder.decode("testSpanName source=spanSource spanId=spanid traceId=traceid 1532012145123456 1532012146234567",
        out);
    assertEquals(expectedTimeUnit.convert(startTs, TimeUnit.MILLISECONDS), (long) out.get(0).getStartMillis());
    assertEquals(expectedTimeUnit.convert(duration, TimeUnit.MILLISECONDS), (long) out.get(0).getDuration());
    out.clear();

    // nanoseconds with duration
    decoder.decode("testSpanName source=spanSource spanId=spanid traceId=traceid 1532012145123456789 1111111111",
        out);
    assertEquals(expectedTimeUnit.convert(startTs, TimeUnit.MILLISECONDS), (long) out.get(0).getStartMillis());
    assertEquals(expectedTimeUnit.convert(duration, TimeUnit.MILLISECONDS), (long) out.get(0).getDuration());
    out.clear();

    // microseconds with end_ts
    decoder.decode("testSpanName source=spanSource spanId=spanid traceId=traceid 1532012145123456 1532012146234567 ",
        out);
    assertEquals(expectedTimeUnit.convert(startTs, TimeUnit.MILLISECONDS), (long) out.get(0).getStartMillis());
    assertEquals(expectedTimeUnit.convert(duration, TimeUnit.MILLISECONDS), (long) out.get(0).getDuration());
    out.clear();

    // microseconds with duration
    decoder.decode("testSpanName source=spanSource spanId=spanid traceId=traceid 1532012145123456 1111111 ",
        out);
    assertEquals(expectedTimeUnit.convert(startTs, TimeUnit.MILLISECONDS), (long) out.get(0).getStartMillis());
    assertEquals(expectedTimeUnit.convert(duration, TimeUnit.MILLISECONDS), (long) out.get(0).getDuration());
    out.clear();

    // milliseconds with end_ts
    decoder.decode("testSpanName source=spanSource spanId=spanid traceId=traceid 1532012145123 1532012146234",
        out);
    assertEquals(expectedTimeUnit.convert(startTs, TimeUnit.MILLISECONDS), (long) out.get(0).getStartMillis());
    assertEquals(expectedTimeUnit.convert(duration, TimeUnit.MILLISECONDS), (long) out.get(0).getDuration());
    out.clear();

    // milliseconds with duration
    decoder.decode("testSpanName source=spanSource spanId=spanid traceId=traceid 1532012145123 1111",
        out);
    assertEquals(expectedTimeUnit.convert(startTs, TimeUnit.MILLISECONDS), (long) out.get(0).getStartMillis());
    assertEquals(expectedTimeUnit.convert(duration, TimeUnit.MILLISECONDS), (long) out.get(0).getDuration());
    out.clear();

    // seconds with end_ts
    decoder.decode("testSpanName source=spanSource spanId=spanid traceId=traceid 1532012145 1532012146",
        out);
    assertEquals(expectedTimeUnit.convert(startTs / 1000, TimeUnit.SECONDS), (long) out.get(0).getStartMillis());
    assertEquals(expectedTimeUnit.convert(duration/ 1000, TimeUnit.SECONDS), (long) out.get(0).getDuration());
    out.clear();

    // seconds with duration
    decoder.decode("testSpanName source=spanSource spanId=spanid traceId=traceid 1532012145 1",
        out);
    assertEquals(expectedTimeUnit.convert(startTs / 1000, TimeUnit.SECONDS), (long) out.get(0).getStartMillis());
    assertEquals(expectedTimeUnit.convert(duration/ 1000, TimeUnit.SECONDS), (long) out.get(0).getDuration());
    out.clear();
  }
}
