package com.wavefront.agent;

import static com.wavefront.agent.ProxyConfig.ProxyAuthMethod.WAVEFRONT_API_TOKEN;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;

import com.wavefront.agent.auth.CSPAuthConnector;
import com.wavefront.common.LazySupplier;
import com.wavefront.common.NamedThreadFactory;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

/**
 * If the tenant came from CSP, the tenant's token will be frequently updated by the class, which
 * stores tenant-related data.
 *
 * @author Norayr Chaparyan(nchaparyan@vmware.com).
 */
public class TenantInfo implements Runnable {
  private static final String GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE =
      "Failed to get access token from CSP.";
  private static final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(1, new NamedThreadFactory("csp-token-updater"));
  private static final Logger log = Logger.getLogger(TenantInfo.class.getCanonicalName());

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
  private final String wfServer;
  private final ProxyConfig.ProxyAuthMethod proxyAuthMethod;
  private String bearerToken;
  private CSPAuthConnector cspAuthConnector;

  public TenantInfo(
      @Nonnull final String token,
      @Nonnull final String wfServer,
      ProxyConfig.ProxyAuthMethod proxyAuthMethod) {
    this(null, null, null, wfServer, token, proxyAuthMethod);
  }

  public TenantInfo(
      @Nullable final String appId,
      @Nullable final String appSecret,
      @Nullable final String orgId,
      @Nonnull final String wfServer,
      ProxyConfig.ProxyAuthMethod proxyAuthMethod) {
    this(appId, appSecret, orgId, wfServer, null, proxyAuthMethod);
  }

  private TenantInfo(
      @Nullable final String appId,
      @Nullable final String appSecret,
      @Nullable final String orgId,
      @Nonnull final String wfServer,
      @Nullable final String token,
      ProxyConfig.ProxyAuthMethod proxyAuthMethod) {

    this.wfServer = wfServer;
    this.proxyAuthMethod = proxyAuthMethod;

    if (proxyAuthMethod == WAVEFRONT_API_TOKEN) {
      this.bearerToken = token;
    } else {
      this.cspAuthConnector = new CSPAuthConnector(appId, appSecret, orgId, token);
    }
  }

  @Nonnull
  public String getWFServer() {
    return wfServer;
  }

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
        case CSP_CLIENT_CREDENTIALS:
          next = processResponse(cspAuthConnector.loadAccessTokenByClientCredentials());
          break;
        case CSP_API_TOKEN:
          next = processResponse(cspAuthConnector.loadAccessTokenByAPIToken());
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

  private long processResponse(final Response response) {
    Metrics.newCounter(
            new MetricName(
                "csp.token.update.response", "", "" + response.getStatusInfo().getStatusCode()))
        .inc();
    long nextIn = 10;
    if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
      errors.get().inc();
      log.severe(
          GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE
              + "("
              + this.proxyAuthMethod
              + ") Status: "
              + response.getStatusInfo().getStatusCode());
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

  /** Calling the function should only be done for testing purposes. */
  void setCSPAuthConnector(@Nonnull final CSPAuthConnector cspAuthConnector) {
    this.cspAuthConnector = cspAuthConnector;
  }
}
