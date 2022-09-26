package com.wavefront.agent.core.buffers;

public class MemoryBufferConfig {
  public int msgRetry = 3;
  public long msgExpirationTime = -1;
  public long maxMemory = -1;
}
