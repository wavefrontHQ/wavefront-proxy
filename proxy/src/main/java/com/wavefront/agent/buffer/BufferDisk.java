package com.wavefront.agent.buffer;

import java.util.logging.Logger;

class BufferDisk extends BufferActiveMQ {
  private static final Logger logger = Logger.getLogger(BufferDisk.class.getCanonicalName());

  public BufferDisk(int level, String name, String buffer) {
    super(level, name, true, buffer);
  }
}
