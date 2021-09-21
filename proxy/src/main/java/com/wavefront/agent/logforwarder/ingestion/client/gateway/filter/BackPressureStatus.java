package com.wavefront.agent.logforwarder.ingestion.client.gateway.filter;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 2:34 PM
 */
public class BackPressureStatus {
  public boolean isActive;
  public int queueSize;
  public int numDroppedRequests;

  public BackPressureStatus() {
  }
}
