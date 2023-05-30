package com.wavefront.agent;

import static com.wavefront.agent.ProxyConfigDef.cspBaseUrl;
import static java.lang.String.format;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;

import com.google.common.net.HttpHeaders;
import com.wavefront.common.NamedThreadFactory;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;

/**
 * The class for keeping tenant required information token, server.
 *
 * @author Norayr Chaparyan (nchaparyan@vmware.com).
 */
public class TenantInfo {
  private static final String GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE =
      "Failed to get access token from CSP.";
  private static final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(1, new NamedThreadFactory("token-configuration"));
  private String bearerToken;
  private final String server;

  public TenantInfo(
      @Nonnull final String token,
      @Nonnull final String server,
      ProxyConfig.ProxyAuthMethod proxyAuthMethod) {
    this.server = server;
    switch (proxyAuthMethod) {
      case CSP_API_TOKEN:
        loadAccessTokenByAPIToken(token, 0);
        break;
      case WAVEFRONT_API_TOKEN:
        bearerToken = token;
        break;
      default:
        throw new IllegalStateException(
            "CSP_API_TOKEN or WAVEFRONT_API_TOKEN should be provided as proxy auth method.");
    }
  }

  public TenantInfo(
      @Nonnull final String clientId,
      @Nonnull final String clientSecret,
      @Nonnull final String orgId,
      @Nonnull final String server) {
    this.server = server;
    loadAccessTokenByClientCredentials(clientId, clientSecret, orgId, 0);
  }

  @Nonnull
  public String getServer() {
    return server;
  }

  @Nonnull
  public String getToken() {
    return bearerToken;
  }

  /**
   * When the CSP access token has expired, obtain it using the CSP api token.
   *
   * @param apiToken the CSP api token.
   * @param failedRequestsCount the number of unsuccessful requests.
   * @throws RuntimeException when CSP is down or wrong CSP api token.
   */
  private void loadAccessTokenByAPIToken(@Nonnull final String apiToken, int failedRequestsCount) {

    Form requestBody = new Form();
    requestBody.param("grant_type", "api_token");
    requestBody.param("api_token", apiToken);

    Response response =
        new ResteasyClientBuilder()
            .build()
            .target(format("%s/csp/gateway/am/api/auth/api-tokens/authorize", cspBaseUrl))
            .request()
            .post(Entity.form(requestBody));

    if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
      if (failedRequestsCount < 3) {
        // If the attempt to get a new access token fails, we'll try three more times because
        // we have at least three minutes until the access token expires.
        executor.schedule(
            () -> loadAccessTokenByAPIToken(apiToken, failedRequestsCount + 1),
            60,
            TimeUnit.SECONDS);
        return;
      } else {
        throw new RuntimeException(
            GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Status: " + response.getStatus());
      }
    }

    final TokenExchangeResponseDTO tokenExchangeResponseDTO =
        response.readEntity(TokenExchangeResponseDTO.class);

    if (tokenExchangeResponseDTO == null) {
      if (failedRequestsCount < 3) {
        // If the attempt to get a new access token fails, we'll try three more times because
        // we have at least three minutes until the access token expires.
        executor.schedule(
            () -> loadAccessTokenByAPIToken(apiToken, failedRequestsCount + 1),
            60,
            TimeUnit.SECONDS);
        return;
      } else {
        throw new RuntimeException(
            GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Status: " + response.getStatus());
      }
    }

    this.bearerToken = tokenExchangeResponseDTO.getAccessToken();
    executor.schedule(
        () -> loadAccessTokenByAPIToken(apiToken, 0),
        getTimeOffset(tokenExchangeResponseDTO.getExpiresIn()),
        TimeUnit.SECONDS);
  }

  /**
   * When the CSP access token has expired, obtain it using server to server OAuth app's client id
   * and client secret.
   *
   * @param clientId a server-to-server OAuth app's client id.
   * @param clientSecret a server-to-server OAuth app's client secret.
   * @param orgId the CSP organisation id.
   * @param failedRequestsCount the number of unsuccessful requests.
   * @throws RuntimeException when CSP is down or wrong csp client credentials.
   */
  private void loadAccessTokenByClientCredentials(
      @Nonnull final String clientId,
      @Nonnull final String clientSecret,
      @Nonnull final String orgId,
      int failedRequestsCount) {

    Form requestBody = new Form();
    requestBody.param("grant_type", "client_credentials");
    requestBody.param("orgId", orgId);

    Response response =
        new ResteasyClientBuilder()
            .build()
            .target(format("%s/csp/gateway/am/api/auth/authorize", cspBaseUrl))
            .request()
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED)
            .header(
                HttpHeaders.AUTHORIZATION,
                buildClientAuthorizationHeaderValue(clientId, clientSecret))
            .post(Entity.form(requestBody));

    if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
      if (failedRequestsCount < 3) {
        // If the attempt to get a new access token fails, we'll try three more times because
        // we have at least three minutes until the access token expires.
        executor.schedule(
            () ->
                loadAccessTokenByClientCredentials(
                    clientId, clientSecret, orgId, failedRequestsCount + 1),
            60,
            TimeUnit.SECONDS);
        return;
      } else {
        throw new RuntimeException(
            GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Status: " + response.getStatus());
      }
    }

    final TokenExchangeResponseDTO tokenExchangeResponseDTO =
        response.readEntity(TokenExchangeResponseDTO.class);

    if (tokenExchangeResponseDTO == null) {
      if (failedRequestsCount < 3) {
        // If the attempt to get a new access token fails, we'll try three more times because
        // we have at least three minutes until the access token expires.
        executor.schedule(
            () ->
                loadAccessTokenByClientCredentials(
                    clientId, clientSecret, orgId, failedRequestsCount + 1),
            60,
            TimeUnit.SECONDS);
        return;
      } else {
        throw new RuntimeException(
            GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Status: " + response.getStatus());
      }
    }

    this.bearerToken = tokenExchangeResponseDTO.getAccessToken();
    executor.schedule(
        () -> loadAccessTokenByClientCredentials(clientId, clientSecret, orgId, 0),
        getTimeOffset(tokenExchangeResponseDTO.getExpiresIn()),
        TimeUnit.SECONDS);
  }

  private String buildClientAuthorizationHeaderValue(
      final String clientId, final String clientSecret) {
    return String.format(
        "Basic %s",
        Base64.getEncoder()
            .encodeToString(String.format("%s:%s", clientId, clientSecret).getBytes()));
  }

  /**
   * Calculates the time offset for scheduling regular requests to a CSP based on the expiration
   * time of a CSP access token.
   *
   * @param expiresIn the expiration time of the CSP access token in seconds.
   * @return the calculated time offset.
   */
  private int getTimeOffset(int expiresIn) {
    if (expiresIn < 600) {
      // If the access token expiration time is less than 10 minutes,
      // schedule requests 30 seconds before it expires.
      return expiresIn - 30;
    }
    // If the access token expiration time is 10 minutes or more,
    // schedule requests 3 minutes before it expires.
    return expiresIn - 180;
  }
}
