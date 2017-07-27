package com.wavefront.common;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import wavefront.report.ReportPoint;

import static org.junit.Assert.assertEquals;

public class ReportPointTest {

  /**
   * This captures an issue where the latest Avro vm templates does not generate overloaded settings for Avro objects
   * and builders.
   */
  @Test
  public void testReportPointBuilderSetters() throws IOException {
    // test integer to long widening conversion.
    ReportPoint rp = ReportPoint.newBuilder().
        setValue(1).
        setMetric("hello").
        setTimestamp(12345).
        build();
    ByteBuffer byteBuffer = rp.toByteBuffer();
    byteBuffer.rewind();
    assertEquals(1L, ReportPoint.fromByteBuffer(byteBuffer).getValue());

    // test long (no widening conversion.
    rp = ReportPoint.newBuilder().
        setValue(1L).
        setMetric("hello").
        setTimestamp(12345).
        build();
    byteBuffer = rp.toByteBuffer();
    byteBuffer.rewind();
    assertEquals(1L, ReportPoint.fromByteBuffer(byteBuffer).getValue());

    // test float to double widening conversion.
    rp = ReportPoint.newBuilder().
        setValue(1f).
        setMetric("hello").
        setTimestamp(12345).
        build();
    byteBuffer = rp.toByteBuffer();
    byteBuffer.rewind();
    assertEquals(1.0D, ReportPoint.fromByteBuffer(byteBuffer).getValue());

    // test long (no widening conversion.
    rp = ReportPoint.newBuilder().
        setValue(1.0D).
        setMetric("hello").
        setTimestamp(12345).
        build();
    byteBuffer = rp.toByteBuffer();
    byteBuffer.rewind();
    assertEquals(1.0D, ReportPoint.fromByteBuffer(byteBuffer).getValue());
  }
}
