package com.wavefront.agent.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * {@link TokenIntrospectionAuthenticator} that validates tokens against an OAuth 2.0-compliant
 * Token Introspection endpoint, as described in <a href="https://tools.ietf.org/html/rfc7662">RFC 7662</a>.
 *
 * @author vasily@wavefront.com
 */
class Oauth2TokenIntrospectionAuthenticator extends TokenIntrospectionAuthenticator {
  private static final Logger logger = Logger.getLogger(Oauth2TokenIntrospectionAuthenticator.class.getCanonicalName());

  private final HttpClient httpClient;
  private final String tokenIntrospectionServiceUrl;
  private final String tokenIntrospectionAuthorizationHeader;

  private final Counter accessGrantedResponses = Metrics.newCounter(new MetricName("auth", "", "access-granted"));
  private final Counter accessDeniedResponses = Metrics.newCounter(new MetricName("auth", "", "access-denied"));

  private static final ObjectMapper JSON_PARSER = new ObjectMapper();

  Oauth2TokenIntrospectionAuthenticator(@Nonnull HttpClient httpClient, @Nonnull String tokenIntrospectionServiceUrl,
                                        @Nullable String tokenIntrospectionAuthorizationHeader,
                                        int authResponseRefreshInterval, int authResponseMaxTtl) {
    this(httpClient, tokenIntrospectionServiceUrl, tokenIntrospectionAuthorizationHeader, authResponseRefreshInterval,
        authResponseMaxTtl, System::currentTimeMillis);
  }

  @VisibleForTesting
  Oauth2TokenIntrospectionAuthenticator(@Nonnull HttpClient httpClient, @Nonnull String tokenIntrospectionServiceUrl,
                                        @Nullable String tokenIntrospectionAuthorizationHeader,
                                        int authResponseRefreshInterval, int authResponseMaxTtl,
                                        @Nonnull Supplier<Long> timeSupplier) {
    super(authResponseRefreshInterval, authResponseMaxTtl, timeSupplier);
    this.httpClient = httpClient;
    this.tokenIntrospectionServiceUrl = tokenIntrospectionServiceUrl;
    this.tokenIntrospectionAuthorizationHeader = tokenIntrospectionAuthorizationHeader;
  }

  @Override
  boolean callAuthService(@NotNull String token) throws Exception {
    boolean result;
    HttpPost request = new HttpPost(tokenIntrospectionServiceUrl.replace("{{token}}", token));
    request.setHeader("Content-Type", "application/x-www-form-urlencoded");
    request.setHeader("Accept", "application/json");
    if (tokenIntrospectionAuthorizationHeader != null) {
      request.setHeader("Authorization", tokenIntrospectionAuthorizationHeader);
    }
    request.setEntity(new UrlEncodedFormEntity(ImmutableList.of(new BasicNameValuePair("token", token))));
    HttpResponse response = httpClient.execute(request);
    JsonNode node = JSON_PARSER.readTree(EntityUtils.toString(response.getEntity()));
    if (node.hasNonNull("active") && node.get("active").isBoolean()) {
      result = node.get("active").asBoolean();
    } else {
      throw new RuntimeException("Response does not contain 'active' field");
    }
    if (result) {
      accessGrantedResponses.inc();
    } else {
      accessDeniedResponses.inc();
    }
    return result;
  }
}
