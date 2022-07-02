package com.wavefront.agent.core.senders;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.LogDataSubmissionTask;
import com.wavefront.api.LogAPI;
import java.util.List;
import java.util.UUID;

/**
 * This class is responsible for accumulating logs and uploading them in batches.
 *
 * @author amitw@vmware.com
 */
public class LogSenderTask extends AbstractSenderTask {
  private final com.wavefront.agent.core.queues.QueueInfo queue;
  private final LogAPI logAPI;
  private final UUID proxyId;
  private final EntityProperties properties;

  /**
   * @param handlerKey handler key, that serves as an identifier of the log pipeline.
   * @param logAPI handles interaction with log systems as well as queueing.
   * @param proxyId id of the proxy.
   * @param properties container for mutable proxy settings.
   * @param buffer
   */
  LogSenderTask(
      com.wavefront.agent.core.queues.QueueInfo handlerKey,
      LogAPI logAPI,
      UUID proxyId,
      EntityProperties properties,
      Buffer buffer) {
    super(handlerKey, properties, buffer);
    this.queue = handlerKey;
    this.logAPI = logAPI;
    this.proxyId = proxyId;
    this.properties = properties;
  }

  // TODO: review
  @Override
  public int processSingleBatch(List<String> batch) {
    LogDataSubmissionTask task =
        new LogDataSubmissionTask(logAPI, proxyId, properties, queue, batch, null);
    return task.execute();
  }
}
