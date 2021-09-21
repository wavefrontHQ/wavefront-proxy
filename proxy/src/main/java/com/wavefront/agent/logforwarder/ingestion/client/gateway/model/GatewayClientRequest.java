package com.wavefront.agent.logforwarder.ingestion.client.gateway.model;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 2:21 PM
 */
public class GatewayClientRequest {
  public URI uri;
  public Object body;
  public byte[] bodyBytes;
  public String contentType;
  public String action;
  public long receivedTimeMicros;
  public Map<String, String> headers;
  public int retryCount;

  public GatewayClientRequest() {
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      GatewayClientRequest that = (GatewayClientRequest)o;
      return this.receivedTimeMicros == that.receivedTimeMicros && this.retryCount == that.retryCount && Objects.equals(this.uri, that.uri) && Objects.equals(this.body, that.body) && Arrays.equals(this.bodyBytes, that.bodyBytes) && Objects.equals(this.contentType, that.contentType) && Objects.equals(this.action, that.action) && Objects.equals(this.headers, that.headers);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[]{this.uri, this.body, this.bodyBytes, this.contentType, this.action, this.receivedTimeMicros, this.headers, this.retryCount});
  }
}
