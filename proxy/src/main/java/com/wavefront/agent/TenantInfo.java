package com.wavefront.agent;

import static com.wavefront.agent.ProxyConfig.PROXY_AUTH_METHOD.CSP_API_TOKEN;
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
  private static final String cspBaseUrl = "https://console-stg.cloud.vmware.com";

  // This can be a Wavefront API token or a CSP API token.
  private String tenantToken;
  private final String tenantServer;
  private String cspAccessToken = null;

  private final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(1, new NamedThreadFactory("token-configuration"));

  public TenantInfo(
      @Nonnull final String tenantToken,
      @Nonnull final String tenantServer,
      ProxyConfig.PROXY_AUTH_METHOD proxyAuthMethod) {
    this.tenantToken = tenantToken;
    this.tenantServer = tenantServer;
    if (proxyAuthMethod == CSP_API_TOKEN) {
      this.cspAccessToken = getAccessTokenByAPIToken();
    }
  }

  public TenantInfo(
      @Nonnull final String serverToServiceClientId,
      @Nonnull final String serverToServiceClientSecret,
      @Nonnull final String orgId,
      @Nonnull final String tenantServer) {
    this.tenantServer = tenantServer;
    this.cspAccessToken =
        getAccessTokenByClientCredentials(
            serverToServiceClientId, serverToServiceClientSecret, orgId);
  }

  @Nonnull
  public String getTenantServer() {
    return tenantServer;
  }

  @Nonnull
  public String getToken() {
    if (cspAccessToken == null) {
      // Returning Wavefront api token.
      return tenantToken;
    }

    return cspAccessToken;
  }

  /**
   * When the CSP access token has expired, obtain it using the CSP api token.
   *
   * @return the CSP access token.
   * @throws RuntimeException when CSP is down or wrong CSP api token.
   */
  @Nonnull
  private String getAccessTokenByAPIToken() {

    Form requestBody = new Form();
    requestBody.param("grant_type", "api_token");
    requestBody.param("api_token", tenantToken);

    Response response =
        new ResteasyClientBuilder()
            .build()
            .target(format("%s/csp/gateway/am/api/auth/api-tokens/authorize", cspBaseUrl))
            .request()
            .post(Entity.form(requestBody));

    if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
      throw new RuntimeException(
          GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Status: " + response.getStatus());
    }

    final TokenExchangeResponseDTO tokenExchangeResponseDTO =
        response.readEntity(TokenExchangeResponseDTO.class);

    if (tokenExchangeResponseDTO == null) {
      throw new RuntimeException(GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Response object is empty.");
    }

    executor.schedule(
        this::getAccessTokenByAPIToken, tokenExchangeResponseDTO.getExpiresIn(), TimeUnit.SECONDS);

    return tokenExchangeResponseDTO.getAccessToken();
  }

  /**
   * When the CSP access token has expired, obtain it using server by server app url and client
   * credentials.
   *
   * @return the CSP access token.
   * @throws RuntimeException when CSP is down or wrong csp client credentials.
   */
  @Nonnull
  private String getAccessTokenByClientCredentials(
      @Nonnull final String serverToServiceClientId,
      @Nonnull final String serverToServiceClientSecret,
      @Nonnull final String orgId) {

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
                buildClientAuthorizationHeaderValue(
                    serverToServiceClientId, serverToServiceClientSecret))
            .post(Entity.form(requestBody));

    if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
      throw new RuntimeException(
          GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Status: " + response.getStatus());
    }

    final TokenExchangeResponseDTO tokenExchangeResponseDTO =
        response.readEntity(TokenExchangeResponseDTO.class);

    if (tokenExchangeResponseDTO == null) {
      throw new RuntimeException(GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Response object is empty.");
    }

    executor.schedule(
        this::getAccessTokenByAPIToken, tokenExchangeResponseDTO.getExpiresIn(), TimeUnit.SECONDS);

    return tokenExchangeResponseDTO.getAccessToken();
  }

  private String buildClientAuthorizationHeaderValue(
      final String clientId, final String clientSecret) {
    return String.format(
        "Basic %s",
        Base64.getEncoder()
            .encodeToString(String.format("%s:%s", clientId, clientSecret).getBytes()));
  }
}
