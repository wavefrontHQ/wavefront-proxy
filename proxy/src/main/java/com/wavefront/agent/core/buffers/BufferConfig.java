package com.wavefront.agent.core.buffers;

public class BufferConfig {
  public String buffer = "";
  public int msgRetry = 1;
  public long msgExpirationTime = 60_000;
}
