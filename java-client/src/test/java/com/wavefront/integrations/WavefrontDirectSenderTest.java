package com.wavefront.integrations;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNull;

/**
 * Tests for {@link WavefrontDirectSender}
 *
 * @author Vikram Raman (vikram@wavefront.com).
 */
public class WavefrontDirectSenderTest {

  @Test
  public void testPointToString() {
    assertNull(WavefrontDirectSender.pointToString(null, 0.0, null, "source", null));
    assertNull(WavefrontDirectSender.pointToString("name", 0.0, null, null, null));

    Assert.assertEquals("\"name\" 10 1469751813 source=\"source\" \"foo\"=\"bar\" \"bar\"=\"foo\"",
        WavefrontDirectSender.pointToString("name",1469751813000L, 10L, "source",
            ImmutableMap.of("foo", "bar", "bar", "foo")));
  }
}
