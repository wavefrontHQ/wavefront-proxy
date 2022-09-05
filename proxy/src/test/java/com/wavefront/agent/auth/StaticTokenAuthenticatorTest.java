package com.wavefront.agent.auth;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

public class StaticTokenAuthenticatorTest {

  @Test
  public void testValidTokenWorks() {
    TokenAuthenticator authenticator = new StaticTokenAuthenticator("staticToken");
    assertTrue(authenticator.authRequired());

    // null should fail
    assertFalse(authenticator.authorize(null));

    // empty string should fail
    assertFalse(authenticator.authorize(""));

    // correct token should work
    assertTrue(authenticator.authorize("staticToken"));

    // numeric string should fail
    assertFalse(authenticator.authorize("123456"));

    // random string should fail
    assertFalse(authenticator.authorize(RandomStringUtils.randomAlphanumeric(36)));
  }
}
