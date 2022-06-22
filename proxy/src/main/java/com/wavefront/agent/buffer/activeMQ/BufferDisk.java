package com.wavefront.agent.buffer.activeMQ;

import com.wavefront.agent.buffer.Buffer;
import com.wavefront.agent.buffer.BufferConfig;
import com.wavefront.agent.buffer.QueueInfo;
import java.util.logging.Logger;

public class BufferDisk extends BufferActiveMQ implements Buffer {
  private static final Logger logger = Logger.getLogger(BufferDisk.class.getCanonicalName());

  public BufferDisk(int level, String name, BufferConfig cfg) {
    super(level, name, true, cfg);
  }

  @Override
  public void createBridge(String target, QueueInfo queue, int level) {}
}
