package com.wavefront.agent.buffer;

import java.util.logging.Logger;

public class DiskBuffer extends ActiveMQBuffer implements Buffer, BufferBatch {
  private static final Logger logger = Logger.getLogger(DiskBuffer.class.getCanonicalName());

  public DiskBuffer(int level, String name, BufferConfig cfg) {
    super(level, name, true, cfg);
  }

  @Override
  public void createBridge(String target, QueueInfo queue, int level) {}
}
