package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:42 AM
 */
public class BufferThreadLocal extends ThreadLocal<byte[]> {
  public BufferThreadLocal() {
  }

  protected byte[] initialValue() {
    return new byte[4096];
  }
}
