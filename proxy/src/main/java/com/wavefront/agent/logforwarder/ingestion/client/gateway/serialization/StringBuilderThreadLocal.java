package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:36 AM
 */
public class StringBuilderThreadLocal extends ThreadLocal<StringBuilder> {
  private static final int BUFFER_INITIAL_CAPACITY = 1024;

  public StringBuilderThreadLocal() {
  }

  protected StringBuilder initialValue() {
    return new StringBuilder(1024);
  }

  public StringBuilder get() {
    StringBuilder result = (StringBuilder)super.get();
    if (result.length() > 10240) {
      result = new StringBuilder(1024);
      this.set(result);
    } else {
      result.setLength(0);
    }

    return result;
  }
}

