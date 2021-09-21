package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 2:00 PM
 */
import com.esotericsoftware.kryo.io.Output;

public final class OutputWithRoot extends Output {
  private Object root;

  public OutputWithRoot(byte[] buffer, int maxSize, Object o) {
    super(buffer, maxSize);
    this.root = o;
  }

  public Object getRoot() {
    return this.root;
  }
}

