package com.wavefront.agent.logforwarder.ingestion.client.gateway.model.exception;

import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayResponse;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/20/21 1:49 PM
 */
public class GatewayException extends RuntimeException {
  public static final long serialVersionUID = 1L;
  private GatewayResponse gatewayResponse;

  public GatewayException(GatewayResponse response, String message, Throwable cause) {
    super(message, cause);
    this.gatewayResponse = response;
  }

  public GatewayResponse getGatewayResponse() {
    return this.gatewayResponse;
  }
}