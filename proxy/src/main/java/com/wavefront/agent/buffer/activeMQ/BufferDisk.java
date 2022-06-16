package com.wavefront.agent.buffer.activeMQ;

import com.wavefront.agent.buffer.SecondaryBuffer;
import com.wavefront.agent.handlers.HandlerKey;
import java.util.logging.Logger;

public class BufferDisk extends BufferActiveMQ implements SecondaryBuffer {
  private static final Logger logger = Logger.getLogger(BufferDisk.class.getCanonicalName());

  public BufferDisk(int level, String name, String buffer) {
    super(level, name, true, buffer);
  }

  @Override
  public void createBridge(HandlerKey key, int level) {}
}
