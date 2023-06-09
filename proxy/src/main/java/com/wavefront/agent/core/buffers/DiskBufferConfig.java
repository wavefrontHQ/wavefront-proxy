package com.wavefront.agent.core.buffers;

import java.io.File;
import java.io.FileNotFoundException;

public class DiskBufferConfig {
  public File buffer;
  public long maxMemory;

  public void validate() {
    if (!buffer.exists() || !buffer.isDirectory()) {
      throw new IllegalArgumentException(
          new FileNotFoundException("Buffer directory '" + buffer + "' Not Found"));
    }
  }
}
