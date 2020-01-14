package com.wavefront.agent.auth;

/**
 * Auth validation methods supported.
 *
 * @author vasily@wavefront.com
 */
public enum TokenValidationMethod {
  NONE, STATIC_TOKEN, HTTP_GET, OAUTH2;

  public static TokenValidationMethod fromString(String name) {
    for (TokenValidationMethod method : TokenValidationMethod.values()) {
      if (method.toString().equalsIgnoreCase(name)) {
        return method;
      }
    }
    return null;
  }
}
