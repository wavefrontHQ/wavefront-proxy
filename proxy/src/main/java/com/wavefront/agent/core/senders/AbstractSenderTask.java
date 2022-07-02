package com.wavefront.agent.core.senders;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.data.EntityProperties;
import java.util.List;

abstract class AbstractSenderTask implements SenderTask, Runnable {
  private com.wavefront.agent.core.queues.QueueInfo queue;
  private EntityProperties properties;
  private Buffer buffer;

  AbstractSenderTask(
      com.wavefront.agent.core.queues.QueueInfo queue, EntityProperties properties, Buffer buffer) {
    this.queue = queue;
    this.properties = properties;
    this.buffer = buffer;
  }

  @Override
  public void run() {
    // TODO: review getDataPerBatch and getRateLimiter
    buffer.onMsgBatch(
        queue, properties.getDataPerBatch(), properties.getRateLimiter(), this::processBatch);
  }

  private void processBatch(List<String> batch) throws Exception {
    int result = processSingleBatch(batch);
    if (result != 0) {
      // TODO: review Exception
      throw new Exception("Error rending point to the server, error code:" + result);
    }
  }
}
