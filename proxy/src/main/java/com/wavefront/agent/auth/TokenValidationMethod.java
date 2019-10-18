package com.wavefront.agent.auth;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

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

  public class TokenValidationMethodConverter implements IStringConverter<TokenValidationMethod> {
    @Override
    public TokenValidationMethod convert(String value) {
      TokenValidationMethod convertedValue = TokenValidationMethod.fromString(value);
      if (convertedValue == null) {
        throw new ParameterException("Unknown token validation method value: " + value);
      }
      return convertedValue;
    }
  }
}
