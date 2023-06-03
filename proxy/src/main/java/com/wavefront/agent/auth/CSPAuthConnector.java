package com.wavefront.agent.auth;

import static com.wavefront.agent.ProxyConfigDef.cspBaseUrl;
import static java.lang.String.format;

import com.google.common.net.HttpHeaders;
import java.util.Base64;
import javax.annotation.Nullable;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl;

/**
 * Tenants that have been onboarded by CSP can request a CSP access token in one of two ways using
 * the CSP auth connector class.
 *
 * @author Norayr Chaparyan(nchaparyan@vmware.com).
 */
public class CSPAuthConnector {
  // CSP api token
  private final String apiToken;
  // CSP organisation id
  private final String orgId;
  // CSP server to server OAuth app secret
  private final String appSecret;
  // CSP server to server OAuth app id
  private final String appId;

  public CSPAuthConnector(
      @Nullable final String appId,
      @Nullable final String appSecret,
      @Nullable final String orgId,
      @Nullable final String apiToken) {
    this.appId = appId;
    this.appSecret = appSecret;
    this.orgId = orgId;
    this.apiToken = apiToken;
  }

  /**
   * When the CSP access token has expired, obtain it using the CSP api token.
   *
   * @throws RuntimeException when CSP is down or wrong CSP api token.
   */
  public Response loadAccessTokenByAPIToken() {
    Form requestBody = new Form();
    requestBody.param("grant_type", "api_token");
    requestBody.param("api_token", apiToken);

    return new ResteasyClientBuilderImpl()
        .build()
        .target(format("%s/csp/gateway/am/api/auth/api-tokens/authorize", cspBaseUrl))
        .request()
        .post(Entity.form(requestBody));
  }

  /**
   * When the CSP access token has expired, obtain it using server to server OAuth app's client id
   * and client secret.
   */
  public Response loadAccessTokenByClientCredentials() {
    Form requestBody = new Form();
    requestBody.param("grant_type", "client_credentials");
    requestBody.param("orgId", orgId);

    String auth =
        String.format(
            "Basic %s",
            Base64.getEncoder()
                .encodeToString(String.format("%s:%s", appId, appSecret).getBytes()));

    return new ResteasyClientBuilderImpl()
        .build()
        .target(format("%s/csp/gateway/am/api/auth/authorize", cspBaseUrl))
        .request()
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED)
        .header(HttpHeaders.AUTHORIZATION, auth)
        .post(Entity.form(requestBody));
  }
}
