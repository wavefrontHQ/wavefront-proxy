package com.wavefront.agent.buffer;

public class BuffersManagerConfig {
  public boolean l2 = true;
  public String buffer = "";
  public int msgRetry = 3;
  public long msgExpirationTime = 60_000;
}
