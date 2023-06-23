package com.wavefront.agent;

import javax.annotation.Nonnull;

public class TokenWorkerWF implements TokenWorker, TenantInfo {
  private String token;
  private String server;

  public TokenWorkerWF(@Nonnull final String token, @Nonnull final String server) {
    this.token = token;
    this.server = server;
  }

  @Override
  public String getWFServer() {
    return server;
  }

  @Override
  public String getBearerToken() {
    return token;
  }

  @Override
  public String getLeMansServer() {
    return null;
  }
}
