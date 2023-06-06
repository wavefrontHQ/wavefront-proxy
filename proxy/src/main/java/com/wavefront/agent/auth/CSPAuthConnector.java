package com.wavefront.agent.auth;

import com.wavefront.agent.api.CSPAPI;
import java.util.Base64;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

/**
 * Tenants that have been onboarded by CSP can request a CSP access token in one of two ways using
 * the CSP auth connector class.
 *
 * @author Norayr Chaparyan(nchaparyan@vmware.com).
 */
public class CSPAuthConnector {
  public static CSPAPI api;

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
    return api.getTokenByAPIToken("api_token", apiToken);
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
    return api.getTokenByClientCredentials(auth, orgId);
  }
}
