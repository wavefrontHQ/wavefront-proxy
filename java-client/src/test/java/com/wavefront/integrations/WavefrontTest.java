package com.wavefront.integrations;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Wavefront}
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class WavefrontTest {

  @Test
  public void testSanitize() {
    assertEquals("\"hello\"", Wavefront.sanitize("hello"));
    assertEquals("\"hello-world\"", Wavefront.sanitize("hello world"));
    assertEquals("\"hello.world\"", Wavefront.sanitize("hello.world"));
    assertEquals("\"hello\\\"world\\\"\"", Wavefront.sanitize("hello\"world\""));
    assertEquals("\"hello'world\"", Wavefront.sanitize("hello'world"));
  }
}
