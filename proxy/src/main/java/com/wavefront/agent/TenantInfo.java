package com.wavefront.agent;

import static com.wavefront.agent.ProxyConfig.ProxyAuthMethod.WAVEFRONT_API_TOKEN;
import static com.wavefront.agent.ProxyConfigDef.cspBaseUrl;
import static java.lang.String.format;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;

import com.google.common.net.HttpHeaders;
import com.wavefront.common.LazySupplier;
import com.wavefront.common.NamedThreadFactory;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl;

public class TenantInfo implements Runnable {
  private static final String GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE =
      "Failed to get access token from CSP.";
  private static final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(1, new NamedThreadFactory("csp-token-updater"));
  private static final Logger log = Logger.getLogger(ProxyConfig.class.getCanonicalName());

  private static final Supplier<Counter> errors =
      LazySupplier.of(() -> Metrics.newCounter(new MetricName("csp.token", "", "exceptions")));
  private static final Supplier<Counter> executions =
      LazySupplier.of(() -> Metrics.newCounter(new MetricName("csp.token", "", "executions")));
  private static final Supplier<Counter> successfully =
      LazySupplier.of(() -> Metrics.newCounter(new MetricName("csp.token", "", "successfully")));
  private static final Supplier<Timer> duration =
      LazySupplier.of(
          () ->
              Metrics.newTimer(
                  new MetricName("csp.token.update", "", "duration"),
                  TimeUnit.MILLISECONDS,
                  TimeUnit.MINUTES));

  // WF or CSP token
  private final String token;

  // CSP id
  private final String orgId;
  private final String clientSecret;
  private final String clientId;

  private final String wfServer;
  private final ProxyConfig.ProxyAuthMethod proxyAuthMethod;
  private String bearerToken;

  public TenantInfo(
      @Nonnull final String token,
      @Nonnull final String wfServer,
      ProxyConfig.ProxyAuthMethod proxyAuthMethod) {
    this(null, null, null, wfServer, token, proxyAuthMethod);
  }

  public TenantInfo(
      @Nonnull final String clientId,
      @Nonnull final String clientSecret,
      @Nonnull final String orgId,
      @Nonnull final String wfServer,
      ProxyConfig.ProxyAuthMethod proxyAuthMethod) {
    this(clientId, clientSecret, orgId, wfServer, null, proxyAuthMethod);
  }

  private TenantInfo(
      @Nonnull final String clientId,
      @Nonnull final String clientSecret,
      @Nonnull final String orgId,
      @Nonnull final String wfServer,
      @Nonnull final String token,
      ProxyConfig.ProxyAuthMethod proxyAuthMethod) {

    this.wfServer = wfServer;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.orgId = orgId;
    this.proxyAuthMethod = proxyAuthMethod;
    this.token = token;

    if (proxyAuthMethod == WAVEFRONT_API_TOKEN) {
      bearerToken = token;
    } else {
      run();
    }
  }

  @Nonnull
  public String getWFServer() {
    return wfServer;
  }

  @Nonnull
  public String getBearerToken() {
    return bearerToken;
  }

  @Override
  public void run() {
    executions.get().inc();
    TimerContext timer = duration.get().time();
    log.info("Updating CSP token " + "(" + this.proxyAuthMethod + ").");
    long next = 10; // in case of unexpected exception
    try {
      switch (proxyAuthMethod) {
        case CSP_CLIENT_CREDENTIALS:
          next = loadAccessTokenByClientCredentials();
          break;
        case CSP_API_TOKEN:
          next = loadAccessTokenByAPIToken();
          break;
      }
    } catch (Throwable e) {
      errors.get().inc();
      log.log(
          Level.SEVERE, GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + "(" + this.proxyAuthMethod + ")", e);
    } finally {
      timer.stop();
      executor.schedule(this, next, TimeUnit.SECONDS);
    }
  }

  /**
   * When the CSP access token has expired, obtain it using the CSP api token.
   *
   * @throws RuntimeException when CSP is down or wrong CSP api token.
   */
  private long loadAccessTokenByAPIToken() {
    Form requestBody = new Form();
    requestBody.param("grant_type", "api_token");
    requestBody.param("api_token", this.token);

    Response response =
        new ResteasyClientBuilderImpl()
            .build()
            .target(format("%s/csp/gateway/am/api/auth/api-tokens/authorize", cspBaseUrl))
            .request()
            .post(Entity.form(requestBody));

    return processResponse(response);
  }

  /**
   * When the CSP access token has expired, obtain it using server to server OAuth app's client id
   * and client secret.
   */
  private long loadAccessTokenByClientCredentials() {
    Form requestBody = new Form();
    requestBody.param("grant_type", "client_credentials");
    requestBody.param("orgId", orgId);

    String auth =
        String.format(
            "Basic %s",
            Base64.getEncoder()
                .encodeToString(String.format("%s:%s", clientId, clientSecret).getBytes()));

    Response response =
        new ResteasyClientBuilderImpl()
            .build()
            .target(format("%s/csp/gateway/am/api/auth/authorize", cspBaseUrl))
            .request()
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED)
            .header(HttpHeaders.AUTHORIZATION, auth)
            .post(Entity.form(requestBody));

    return processResponse(response);
  }

  private long processResponse(final Response response) {
    Metrics.newCounter(
            new MetricName(
                "csp.token.response.code", "", "" + response.getStatusInfo().getStatusCode()))
        .inc();
    long nextIn = 10;
    if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
      log.severe(
          GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE
              + "("
              + this.proxyAuthMethod
              + ") Status: "
              + response.getStatus());
    } else {
      try {
        final TokenExchangeResponseDTO tokenExchangeResponseDTO =
            response.readEntity(TokenExchangeResponseDTO.class);
        this.bearerToken = tokenExchangeResponseDTO.getAccessToken();
        nextIn = getTimeOffset(tokenExchangeResponseDTO.getExpiresIn());
        successfully.get().inc();
      } catch (Throwable e) {
        errors.get().inc();
        log.log(
            Level.SEVERE, GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + "(" + this.proxyAuthMethod + ")", e);
      }
    }
    return nextIn;
  }

  /**
   * Calculates the time offset for scheduling regular requests to a CSP based on the expiration
   * time of a CSP access token. If the access token expiration time is less than 10 minutes,
   * schedule requests 30 seconds before it expires. If the access token expiration time is 10
   * minutes or more, schedule requests 3 minutes before it expires.
   *
   * @param expiresIn the expiration time of the CSP access token in seconds.
   * @return the calculated time offset.
   */
  private int getTimeOffset(int expiresIn) {
    if (expiresIn < 600) {
      return expiresIn - 30;
    }
    return expiresIn - 180;
  }
}
