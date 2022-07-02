package com.wavefront.agent.core.buffers;

public class BufferConfig {
  public String buffer = "";
  public int msgRetry = 3;
  public long msgExpirationTime = 60_000;
}
