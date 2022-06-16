package com.wavefront.agent.buffer;

import com.wavefront.agent.handlers.HandlerKey;

public interface SecondaryBuffer extends Buffer {
  void createBridge(HandlerKey key, int level);
}
