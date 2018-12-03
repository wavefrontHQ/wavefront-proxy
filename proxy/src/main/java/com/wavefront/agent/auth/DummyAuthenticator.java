package com.wavefront.agent.auth;

/**
 * A dummy authenticator for the "No authorization required" flow.
 *
 * @author vasily@wavefront.com
 */
class DummyAuthenticator implements TokenAuthenticator {

  DummyAuthenticator() {
  }

  @Override
  public boolean authorize(String token) {
    return true;
  }

  @Override
  public boolean authRequired() {
    return false;
  }
}
