package com.wavefront.agent;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.api.CSPAPI;
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
import javax.ws.rs.core.Response;

/**
 * If the tenant came from CSP, the tenant's token will be frequently updated by the class, which
 * stores tenant-related data.
 *
 * @author Norayr Chaparyan(nchaparyan@vmware.com).
 */
public class TokenWorkerCSP
    implements TokenWorker, TokenWorker.External, TokenWorker.Scheduled, TenantInfo {
  private static final String GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE =
      "Failed to get access token from CSP (%s). %s";
  private static final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(1, new NamedThreadFactory("csp-token-updater"));
  private static final Logger log = Logger.getLogger(TokenWorkerCSP.class.getCanonicalName());
  private static final Supplier<Counter> errors =
      LazySupplier.of(
          () -> Metrics.newCounter(new MetricName("csp.token.update", "", "exceptions")));
  private static final Supplier<Counter> executions =
      LazySupplier.of(
          () -> Metrics.newCounter(new MetricName("csp.token.update", "", "executions")));
  private static final Supplier<Counter> successfully =
      LazySupplier.of(
          () -> Metrics.newCounter(new MetricName("csp.token.update", "", "successfully")));
  private static final Supplier<Timer> duration =
      LazySupplier.of(
          () ->
              Metrics.newTimer(
                  new MetricName("csp.token.update", "", "duration"),
                  TimeUnit.MILLISECONDS,
                  TimeUnit.MINUTES));

  private enum ProxyAuthMethod {
    API_TOKEN,
    CLIENT_CREDENTIALS
  }

  private final String wfServer;
  private final ProxyAuthMethod proxyAuthMethod;
  private String appId;
  private String appSecret;
  private String orgId;
  private String token;
  private String bearerToken;
  private CSPAPI api;

  public TokenWorkerCSP(
      final String appId, final String appSecret, final String orgId, final String wfServer) {
    this.appId = checkNotNull(appId);
    this.appSecret = checkNotNull(appSecret);
    this.orgId = orgId;
    this.wfServer = checkNotNull(wfServer);
    proxyAuthMethod = ProxyAuthMethod.CLIENT_CREDENTIALS;
  }

  public TokenWorkerCSP(final String token, final String wfServer) {
    this.token = checkNotNull(token);
    this.wfServer = checkNotNull(wfServer);
    proxyAuthMethod = ProxyAuthMethod.API_TOKEN;
  }

  @Override
  public void setAPIContainer(APIContainer apiContainer) {
    api = apiContainer.getCSPApi();
  }

  @Nonnull
  public String getWFServer() {
    return wfServer;
  }

  @Override
  public String getBearerToken() {
    return bearerToken;
  }

  @Override
  public void run() {
    executions.get().inc();
    TimerContext timer = duration.get().time();
    log.info("Updating CSP access token " + "(" + this.proxyAuthMethod + ").");
    long next = 10; // in case of unexpected exception
    try {
      switch (proxyAuthMethod) {
        case CLIENT_CREDENTIALS:
          next = processResponse(loadAccessTokenByClientCredentials());
          break;
        case API_TOKEN:
          next = processResponse(loadAccessTokenByAPIToken());
          break;
      }
    } catch (Throwable e) {
      errors.get().inc();
      log.log(
          Level.SEVERE, GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + "(" + this.proxyAuthMethod + ")", e);
    } finally {
      timer.stop();
      executor.schedule(this::run, next, TimeUnit.SECONDS);
    }
  }

  public Response loadAccessTokenByAPIToken() {
    return api.getTokenByAPIToken("api_token", token);
  }

  /**
   * When the CSP access token has expired, obtain it using server to server OAuth app's client id
   * and client secret.
   */
  public Response loadAccessTokenByClientCredentials() {
    String auth =
        String.format(
            "Basic %s",
            Base64.getEncoder()
                .encodeToString(String.format("%s:%s", appId, appSecret).getBytes()));

    if (Strings.isNullOrEmpty(orgId)) {
      return api.getTokenByClientCredentials(auth, "client_credentials");
    }

    return api.getTokenByClientCredentialsWithOrgId(auth, "client_credentials", orgId);
  }

  private long processResponse(final Response response) {
    Metrics.newCounter(
            new MetricName(
                "csp.token.update.response", "", "" + response.getStatusInfo().getStatusCode()))
        .inc();
    long nextIn = 10;
    if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
      errors.get().inc();
      String jsonString = response.readEntity(String.class);
      // Parse the JSON response
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        JsonNode jsonNode = objectMapper.readTree(jsonString);
        String message = jsonNode.get("message").asText();

        log.severe(
            String.format(GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE, this.proxyAuthMethod, message)
                + ". Status: "
                + response.getStatusInfo().getStatusCode());

      } catch (Exception e) {
        log.severe(
            String.format(GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE, this.proxyAuthMethod, jsonString)
                + ". Status: "
                + response.getStatusInfo().getStatusCode());
      }
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
