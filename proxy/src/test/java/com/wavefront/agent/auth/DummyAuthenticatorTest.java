package com.wavefront.agent.auth;

import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

public class DummyAuthenticatorTest {

  @Test
  public void testAnyTokenWorks() {
    TokenAuthenticator authenticator = new DummyAuthenticator();

    // null should be ok
    assertTrue(authenticator.authorize(null));

    // empty string should be ok
    assertTrue(authenticator.authorize(""));

    // numeric string should be ok
    assertTrue(authenticator.authorize("123456"));

    // random string should be ok
    assertTrue(authenticator.authorize(RandomStringUtils.randomAlphanumeric(36)));
  }
}
