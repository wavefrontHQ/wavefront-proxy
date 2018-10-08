package com.wavefront.agent.auth;

import javax.annotation.Nullable;

/**
 * Token validator for processing incoming requests.
 *
 * @author vasily@wavefront.com
 */
public interface TokenAuthenticator {

  /**
   * Validate a token.
   *
   * @param token token to validate.
   * @return true if the token is considered valid.
   */
  boolean authorize(@Nullable String token);

  /**
   * Check whether authentication is required (i.e. "true" authenticator)
   *
   * @return true if authentication is required.
   */
  boolean authRequired();
}
