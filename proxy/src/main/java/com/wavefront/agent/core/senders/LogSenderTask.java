package com.wavefront.agent.core.senders;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.queues.QueueInfo;
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
  private final QueueInfo queue;
  private final LogAPI logAPI;
  private final UUID proxyId;
  private final EntityProperties properties;
  private SenderStats senderStats;

  /**
   * @param handlerKey handler key, that serves as an identifier of the log pipeline.
   * @param logAPI handles interaction with log systems as well as queueing.
   * @param proxyId id of the proxy.
   * @param properties container for mutable proxy settings.
   * @param buffer
   * @param senderStats
   */
  LogSenderTask(
      QueueInfo handlerKey,
      int idx,
      LogAPI logAPI,
      UUID proxyId,
      EntityProperties properties,
      Buffer buffer,
      SenderStats senderStats) {
    super(handlerKey, idx, properties, buffer);
    this.queue = handlerKey;
    this.logAPI = logAPI;
    this.proxyId = proxyId;
    this.properties = properties;
    this.senderStats = senderStats;
  }

  // TODO: review
  @Override
  public int processSingleBatch(List<String> batch) {
    LogDataSubmissionTask task =
        new LogDataSubmissionTask(logAPI, proxyId, properties, queue, batch, null, senderStats);
    return task.execute();
  }
}
