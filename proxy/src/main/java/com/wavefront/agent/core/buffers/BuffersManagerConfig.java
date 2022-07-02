package com.wavefront.agent.core.buffers;

public class BuffersManagerConfig {
  public boolean l2 = true;
  public String buffer = "";
  public int msgRetry = 3;
  public long msgExpirationTime = 60_000;
}
