package com.wavefront.agent.buffer.activeMQ;

import java.util.logging.Logger;

public class BufferMemory extends BufferActiveMQ {
  private static final Logger logger = Logger.getLogger(BufferMemory.class.getCanonicalName());

  public BufferMemory(int level, String name, String buffer) {
    super(level, name, false, buffer);
  }
}
