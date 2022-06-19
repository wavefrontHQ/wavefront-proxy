package com.wavefront.agent.buffer.activeMQ;

import com.wavefront.agent.buffer.BufferConfig;
import java.util.logging.Logger;

public class BufferMemory extends BufferActiveMQ {
  private static final Logger logger = Logger.getLogger(BufferMemory.class.getCanonicalName());

  public BufferMemory(int level, String name, BufferConfig cfg) {
    super(level, name, false, cfg);
  }
}
