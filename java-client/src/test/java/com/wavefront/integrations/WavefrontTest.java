package com.wavefront.integrations;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Wavefront}
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class WavefrontTest {

  @Test
  public void testCanHandleDistributions() {
    WavefrontSender metricsOnlySender = new Wavefront("localhost", 2878);
    assertFalse(((Wavefront) metricsOnlySender).canHandleDistributions());

    WavefrontSender sender = new Wavefront("localhost", 2878, 40000);
    assertTrue(((Wavefront) sender).canHandleDistributions());
  }
}
