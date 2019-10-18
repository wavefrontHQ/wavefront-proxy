package com.wavefront.agent.auth;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * {@link TokenAuthenticator} that validates tokens by comparing them to a pre-defined value.
 *
 * @author vasily@wavefront.com
 */
class StaticTokenAuthenticator implements TokenAuthenticator {
  private final String staticToken;

  StaticTokenAuthenticator(@Nonnull String staticToken) {
    Preconditions.checkNotNull(staticToken, "staticToken parameter must be set");
    this.staticToken = staticToken;
  }

  @Override
  public boolean authorize(@Nullable String token) {
    return staticToken.equals(token);
  }

  @Override
  public boolean authRequired() {
    return true;
  }
}
