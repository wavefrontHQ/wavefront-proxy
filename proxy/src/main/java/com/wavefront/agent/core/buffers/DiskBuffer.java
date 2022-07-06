package com.wavefront.agent.core.buffers;

import com.wavefront.agent.core.queues.QueueInfo;
import java.util.logging.Logger;
import org.apache.activemq.artemis.api.core.client.*;

public class DiskBuffer extends ActiveMQBuffer implements Buffer, BufferBatch {
  private static final Logger log = Logger.getLogger(DiskBuffer.class.getCanonicalName());

  public DiskBuffer(int level, String name, BufferConfig cfg) {
    super(level, name, true, cfg);
  }

  @Override
  public void createBridge(String target, QueueInfo queue, int level) {}
}
