package com.wavefront.agent.auth;

import org.apache.http.client.HttpClient;

/**
 * Builder for {@link TokenAuthenticator} instances.
 *
 * @author vasily@wavefront.com
 */
public class TokenAuthenticatorBuilder {
  private TokenValidationMethod tokenValidationMethod;
  private HttpClient httpClient;
  private String tokenIntrospectionServiceUrl;
  private String tokenIntrospectionAuthorizationHeader;
  private int authResponseRefreshInterval;
  private int authResponseMaxTtl;
  private String staticToken;

  public static TokenAuthenticatorBuilder create() {
    return new TokenAuthenticatorBuilder();
  }

  private TokenAuthenticatorBuilder() {
    this.tokenValidationMethod = TokenValidationMethod.NONE;
    this.httpClient = null;
    this.tokenIntrospectionServiceUrl = null;
    this.tokenIntrospectionAuthorizationHeader = null;
    this.authResponseRefreshInterval = 600; // 10 min
    this.authResponseMaxTtl = 24 * 60 * 60; // 1 day
    this.staticToken = null;
  }

  public TokenAuthenticatorBuilder setTokenValidationMethod(TokenValidationMethod tokenValidationMethod) {
    this.tokenValidationMethod = tokenValidationMethod;
    return this;
  }

  public TokenAuthenticatorBuilder setHttpClient(HttpClient httpClient) {
    this.httpClient = httpClient;
    return this;
  }

  public TokenAuthenticatorBuilder setTokenIntrospectionServiceUrl(String tokenIntrospectionServiceUrl) {
    this.tokenIntrospectionServiceUrl = tokenIntrospectionServiceUrl;
    return this;
  }

  public TokenAuthenticatorBuilder setTokenIntrospectionAuthorizationHeader(
      String tokenIntrospectionAuthorizationHeader) {
    this.tokenIntrospectionAuthorizationHeader = tokenIntrospectionAuthorizationHeader;
    return this;
  }

  public TokenAuthenticatorBuilder setAuthResponseRefreshInterval(int authResponseRefreshInterval) {
    this.authResponseRefreshInterval = authResponseRefreshInterval;
    return this;
  }

  public TokenAuthenticatorBuilder setAuthResponseMaxTtl(int authResponseMaxTtl) {
    this.authResponseMaxTtl = authResponseMaxTtl;
    return this;
  }

  public TokenAuthenticatorBuilder setStaticToken(String staticToken) {
    this.staticToken = staticToken;
    return this;
  }

  /**
   * @return {@link TokenAuthenticator} instance.
   */
  public TokenAuthenticator build() {
    switch (tokenValidationMethod) {
      case NONE:
        return new DummyAuthenticator();
      case STATIC_TOKEN:
        return new StaticTokenAuthenticator(staticToken);
      case HTTP_GET:
        return new HttpGetTokenIntrospectionAuthenticator(httpClient, tokenIntrospectionServiceUrl,
            tokenIntrospectionAuthorizationHeader, authResponseRefreshInterval, authResponseMaxTtl);
      case OAUTH2:
        return new Oauth2TokenIntrospectionAuthenticator(httpClient, tokenIntrospectionServiceUrl,
            tokenIntrospectionAuthorizationHeader, authResponseRefreshInterval, authResponseMaxTtl);
      default:
        throw new IllegalStateException("Unknown token validation method!");
    }
  }
}
