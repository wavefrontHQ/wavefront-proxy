package com.wavefront.agent;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.net.HttpHeaders;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Form;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.client.JerseyClientBuilder;

import javax.annotation.Nonnull;
import javax.ws.rs.core.MediaType;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static com.wavefront.agent.ProxyConfig.PROXY_AUTH_METHOD.CSP_API_TOKEN;
import static jakarta.ws.rs.core.Response.Status.Family.SUCCESSFUL;
import static java.lang.String.format;

/**
 * The class for keeping tenant required information token, server.
 *
 * @author Norayr Chaparyan (nchaparyan@vmware.com).
 */
public class TenantInfo {
    private static final String GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE = "Failed to get access token from CSP.";
    private static final String CSP_AUTH_TOKEN = "csp-auth-token";
    // The CSP console url.
    private static final String cspBaseUrl = "https://console-stg.cloud.vmware.com";

    // This can be a Wavefront API token or a CSP API token.
    private String tenantToken;
    private final String tenantServer;
    private final LoadingCache<String, TokenExchangeResponseDTO> tokenCache;

    TenantInfo(@Nonnull final String tenantToken,
               @Nonnull final String tenantServer,
               ProxyConfig.PROXY_AUTH_METHOD proxyAuthMethod) {
        this.tenantToken = tenantToken;
        this.tenantServer = tenantServer;
        if (proxyAuthMethod == CSP_API_TOKEN) {
            tokenCache = Caffeine.newBuilder().
                    expireAfter(new Expiry<String, TokenExchangeResponseDTO>() {
                        @Override
                        public long expireAfterCreate(@Nonnull String authToken,
                                                      @Nonnull TokenExchangeResponseDTO tokenExchangeResponseDTO,
                                                      long currentTime) {
                            return TimeUnit.SECONDS.toNanos(tokenExchangeResponseDTO.getExpiresIn());
                        }

                        @Override
                        public long expireAfterUpdate(@Nonnull String authToken, @Nonnull TokenExchangeResponseDTO tokenExchangeResponseDTO,
                                                      long currentTime, long currentDuration) {
                            return currentDuration;
                        }

                        @Override
                        public long expireAfterRead(@Nonnull String authToken, @Nonnull TokenExchangeResponseDTO tokenExchangeResponseDTO,
                                                    long currentTime, long currentDuration) {
                            return currentDuration;
                        }
                    }).build(key -> getAccessTokenByAPIToken());
        } else {
            // This step assumes that the authentication mechanism is WAVEFRONT_API_TOKEN.
            tokenCache = null;
        }
    }

    TenantInfo(@Nonnull final String serverToServiceClientId,
               @Nonnull final String serverToServiceClientSecret,
               @Nonnull final String orgId,
               @Nonnull final String tenantServer) {
        this.tenantServer = tenantServer;
        this.tokenCache = Caffeine.newBuilder().
                expireAfter(new Expiry<String, TokenExchangeResponseDTO>() {
                    @Override
                    public long expireAfterCreate(@Nonnull String authToken,
                                                  @Nonnull TokenExchangeResponseDTO tokenExchangeResponseDTO,
                                                  long currentTime) {
                        return TimeUnit.SECONDS.toNanos(tokenExchangeResponseDTO.getExpiresIn());
                    }

                    @Override
                    public long expireAfterUpdate(@Nonnull String authToken, @Nonnull TokenExchangeResponseDTO tokenExchangeResponseDTO,
                                                  long currentTime, long currentDuration) {
                        return currentDuration;
                    }

                    @Override
                    public long expireAfterRead(@Nonnull String authToken, @Nonnull TokenExchangeResponseDTO tokenExchangeResponseDTO, long currentTime,
                                                long currentDuration) {
                        return currentDuration;
                    }
                }).build(key -> getAccessTokenByClientCredentials(serverToServiceClientId, serverToServiceClientSecret, orgId));
    }

    @Nonnull
    public String getTenantServer() {
        return tenantServer;
    }

    @Nonnull
    public String getToken() {
        if (tokenCache == null) {
            // Returning Wavefront api token.
            return tenantToken;
        }

        return tokenCache.get(CSP_AUTH_TOKEN).getAccessToken();
    }

    /**
     * When the CSP access token has expired, obtain it using the CSP api token.
     *
     * @return the CSP token exchange dto.
     * @throws RuntimeException when CSP is down or wrong CSP api token.
     */
    @Nonnull
    private TokenExchangeResponseDTO getAccessTokenByAPIToken() {
        Form body = new Form();
        body.param("grant_type", "api_token");
        body.param("api_token", tenantToken);

        Response response =
                new JerseyClientBuilder()
                        .build()
                        .target(format("%s/csp/gateway/am/api/auth/api-tokens/authorize", cspBaseUrl))
                        .request()
                        .post(Entity.form(body));

        if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
            throw new RuntimeException(
                    GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Status: " + response.getStatus());
        }

        final TokenExchangeResponseDTO tokenExchangeResponseDTO =
                response.readEntity(TokenExchangeResponseDTO.class);

        if (tokenExchangeResponseDTO == null) {
            throw new RuntimeException(GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Response object is empty.");
        }

        return tokenExchangeResponseDTO;
    }

    /**
     * When the CSP access token has expired, obtain it using server by server app url and client
     * credentials.
     *
     * @return the CSP token exchange dto.
     * @throws RuntimeException when CSP is down or wrong csp client credentials.
     */
    @Nonnull
    private TokenExchangeResponseDTO getAccessTokenByClientCredentials(@Nonnull final String serverToServiceClientId,
                                                                       @Nonnull final String serverToServiceClientSecret,
                                                                       @Nonnull final String orgId) {
        Form body = new Form();
        body.param("grant_type", "client_credentials");
        body.param("orgId", orgId);

        Response response =
                new JerseyClientBuilder()
                        .build()
                        .target(format("%s/csp/gateway/am/api/auth/authorize", cspBaseUrl))
                        .request()
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED)
                        .header(
                                HttpHeaders.AUTHORIZATION,
                                buildClientAuthorizationHeaderValue(
                                        serverToServiceClientId, serverToServiceClientSecret))
                        .post(Entity.form(body));

        if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
            throw new RuntimeException(
                    GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Status: " + response.getStatus());
        }

        final TokenExchangeResponseDTO tokenExchangeResponseDTO =
                response.readEntity(TokenExchangeResponseDTO.class);

        if (tokenExchangeResponseDTO == null) {
            throw new RuntimeException(GET_CSP_ACCESS_TOKEN_ERROR_MESSAGE + " Response object is empty.");
        }

        return tokenExchangeResponseDTO;
    }

    private String buildClientAuthorizationHeaderValue(
            final String clientId, final String clientSecret) {
        return String.format(
                "Basic %s",
                Base64.getEncoder()
                        .encodeToString(String.format("%s:%s", clientId, clientSecret).getBytes()));
    }
}
