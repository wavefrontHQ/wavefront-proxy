package com.wavefront.agent.core.buffers;

public class BuffersManagerConfig {
  public boolean l2 = true;
  public String buffer = "";
  public int msgRetry = 3;
  public long msgExpirationTime = 1_000;
  public long diskMaxMemory = 256_000_000;
  public long memoryMaxMemory = 768_000_000;
}
