package com.wavefront.agent.logforwarder.ingestion.util;

import java.util.Map;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 3:01 PM
 */
public class CustomHttpException extends RuntimeException {

  private int statusCode;

  private Map<String, String> responseHeaders;

  public CustomHttpException(int errCode, Throwable e, String message) {
    super(message, e);
    this.statusCode = errCode;
  }

  public CustomHttpException(int errCode, Throwable e, String message, Map<String, String> responseHeaders) {
    super(message, e);
    this.statusCode = errCode;
    this.responseHeaders = responseHeaders;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public Map<String, String> getResponseHeaders() {
    return responseHeaders;
  }
}
