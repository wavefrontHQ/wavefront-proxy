package com.wavefront.agent;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import javax.annotation.Nonnull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TokenExchangeResponseDTO {

  @JsonProperty("id_token")
  private String idToken;

  @JsonProperty("token_type")
  private String tokenType;

  @JsonProperty("expires_in")
  private int expiresIn;

  private String scope;

  @JsonProperty("access_token")
  private String accessToken;

  @JsonProperty("refresh_token")
  private String refreshToken;

  public int getExpiresIn() {
    return expiresIn;
  }

  @Nonnull
  public String getAccessToken() {
    return accessToken;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TokenExchangeResponseDTO)) {
      return false;
    }
    TokenExchangeResponseDTO that = (TokenExchangeResponseDTO) o;
    return expiresIn == that.expiresIn
        && Objects.equals(accessToken, that.accessToken)
        && Objects.equals(refreshToken, that.refreshToken)
        && Objects.equals(idToken, that.idToken)
        && Objects.equals(tokenType, that.tokenType)
        && Objects.equals(scope, that.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accessToken, refreshToken, idToken, tokenType, expiresIn, scope);
  }

  @Override
  public String toString() {
    return "Token{"
        + "accessToken='"
        + accessToken
        + '\''
        + ", refresh_token='"
        + refreshToken
        + '\''
        + ", idToken='"
        + idToken
        + '\''
        + ", tokenType='"
        + tokenType
        + '\''
        + ", expiresIn="
        + expiresIn
        + ", scope='"
        + scope
        + '\''
        + '}';
  }
}
