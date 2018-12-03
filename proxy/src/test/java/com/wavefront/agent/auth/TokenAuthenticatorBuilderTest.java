package com.wavefront.agent.auth;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TokenAuthenticatorBuilderTest {

  @Test
  public void testBuilderOutput() {
    assertTrue(TokenAuthenticatorBuilder.create().build() instanceof DummyAuthenticator);

    assertTrue(TokenAuthenticatorBuilder.create().
        setTokenValidationMethod(TokenValidationMethod.NONE).build() instanceof DummyAuthenticator);

    assertTrue(TokenAuthenticatorBuilder.create().setTokenValidationMethod(TokenValidationMethod.STATIC_TOKEN).
        setStaticToken("statictoken").build() instanceof StaticTokenAuthenticator);

    HttpClient httpClient = HttpClientBuilder.create().useSystemProperties().build();

    assertTrue(TokenAuthenticatorBuilder.create().setTokenValidationMethod(TokenValidationMethod.HTTP_GET).
        setHttpClient(httpClient).setTokenIntrospectionServiceUrl("https://acme.corp/url").
        build() instanceof HttpGetTokenIntrospectionAuthenticator);

    assertTrue(TokenAuthenticatorBuilder.create().setTokenValidationMethod(TokenValidationMethod.OAUTH2).
        setHttpClient(httpClient).setTokenIntrospectionServiceUrl("https://acme.corp/url").
        build() instanceof Oauth2TokenIntrospectionAuthenticator);
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderStaticTokenIncompleteArgumentsThrows() {
    TokenAuthenticatorBuilder.create().setTokenValidationMethod(TokenValidationMethod.STATIC_TOKEN).build();
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderHttpGetIncompleteArgumentsThrows() {
    TokenAuthenticatorBuilder.create().setTokenValidationMethod(TokenValidationMethod.HTTP_GET).build();
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderHttpGetIncompleteArguments2Throws() {
    TokenAuthenticatorBuilder.create().setTokenValidationMethod(TokenValidationMethod.HTTP_GET).
        setTokenIntrospectionServiceUrl("http://acme.corp").build();
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderOauth2IncompleteArgumentsThrows() {
    TokenAuthenticatorBuilder.create().setTokenValidationMethod(TokenValidationMethod.OAUTH2).build();
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderOauth2IncompleteArguments2Throws() {
    TokenAuthenticatorBuilder.create().setTokenValidationMethod(TokenValidationMethod.OAUTH2).
        setTokenIntrospectionServiceUrl("http://acme.corp").build();
  }
}
