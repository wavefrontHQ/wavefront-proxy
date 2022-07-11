package com.wavefront.agent.core.senders;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.data.EntityProperties;
import java.util.List;
import java.util.logging.Logger;

abstract class AbstractSenderTask implements SenderTask, Runnable {
  private static final Logger log = Logger.getLogger(AbstractSenderTask.class.getCanonicalName());
  private QueueInfo queue;
  private int idx;
  private EntityProperties properties;
  private Buffer buffer;

  AbstractSenderTask(QueueInfo queue, int idx, EntityProperties properties, Buffer buffer) {
    this.queue = queue;
    this.idx = idx;
    this.properties = properties;
    this.buffer = buffer;
  }

  @Override
  public void run() {
    // TODO: review getDataPerBatch and getRateLimiter
    buffer.onMsgBatch(
        queue, idx, properties.getDataPerBatch(), properties.getRateLimiter(), this::processBatch);
  }

  private void processBatch(List<String> batch) throws Exception {
    int result = processSingleBatch(batch);
    if (result != 0) {
      // TODO: review Exception
      throw new Exception("Error sending point to the server, error code:" + result);
    }
  }
}
