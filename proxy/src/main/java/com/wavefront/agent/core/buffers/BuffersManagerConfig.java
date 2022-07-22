package com.wavefront.agent.core.buffers;

public class BuffersManagerConfig {
  public boolean disk = true;
  public boolean external = false;

  public SQSBufferConfig sqsCfg = new SQSBufferConfig();
  public final MemoryBufferConfig memoryCfg = new MemoryBufferConfig();
  public final DiskBufferConfig diskCfg = new DiskBufferConfig();
}
