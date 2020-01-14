package com.wavefront.agent.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * {@link TokenIntrospectionAuthenticator} that considers any 2xx response to be valid. Token to validate is passed in
 * the url, {{token}} placeholder is substituted before the call.
 *
 * @author vasily@wavefront.com
 */
class HttpGetTokenIntrospectionAuthenticator extends TokenIntrospectionAuthenticator {
  private final HttpClient httpClient;
  private final String tokenIntrospectionServiceUrl;
  private final String tokenIntrospectionServiceAuthorizationHeader;

  private final Counter accessGrantedResponses = Metrics.newCounter(new MetricName("auth", "", "access-granted"));
  private final Counter accessDeniedResponses = Metrics.newCounter(new MetricName("auth", "", "access-denied"));

  HttpGetTokenIntrospectionAuthenticator(@Nonnull HttpClient httpClient, @Nonnull String tokenIntrospectionServiceUrl,
                                         @Nullable String tokenIntrospectionServiceAuthorizationHeader,
                                         int authResponseRefreshInterval, int authResponseMaxTtl) {
    this(httpClient, tokenIntrospectionServiceUrl, tokenIntrospectionServiceAuthorizationHeader,
        authResponseRefreshInterval, authResponseMaxTtl, System::currentTimeMillis);

  }

  @VisibleForTesting
  HttpGetTokenIntrospectionAuthenticator(@Nonnull HttpClient httpClient, @Nonnull String tokenIntrospectionServiceUrl,
                                         @Nullable String tokenIntrospectionServiceAuthorizationHeader,
                                         int authResponseRefreshInterval, int authResponseMaxTtl,
                                         @Nonnull Supplier<Long> timeSupplier) {
    super(authResponseRefreshInterval, authResponseMaxTtl, timeSupplier);
    Preconditions.checkNotNull(httpClient, "httpClient must be set");
    Preconditions.checkNotNull(tokenIntrospectionServiceUrl, "tokenIntrospectionServiceUrl parameter must be set");
    this.httpClient = httpClient;
    this.tokenIntrospectionServiceUrl = tokenIntrospectionServiceUrl;
    this.tokenIntrospectionServiceAuthorizationHeader = tokenIntrospectionServiceAuthorizationHeader;
  }

  @Override
  boolean callAuthService(@Nonnull String token) throws Exception {
    HttpGet request = new HttpGet(tokenIntrospectionServiceUrl.replace("{{token}}", token));
    if (tokenIntrospectionServiceAuthorizationHeader != null) {
      request.setHeader("Authorization", tokenIntrospectionServiceAuthorizationHeader);
    }
    HttpResponse response = httpClient.execute(request);
    int status = response.getStatusLine().getStatusCode();
    EntityUtils.consumeQuietly(response.getEntity());
    if (status >= 200 && status < 300) { // all 2xx responses should be considered OK
      accessGrantedResponses.inc();
      return true;
    }
    accessDeniedResponses.inc();
    return false;
  }
}
