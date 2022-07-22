package com.wavefront.agent.core.senders;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.api.ProxyV2API;
import java.util.List;
import java.util.UUID;

/**
 * SenderTask for newline-delimited data.
 *
 * @author vasily@wavefront.com
 */
class LineDelimitedSenderTask extends AbstractSenderTask {

  private final ProxyV2API proxyAPI;
  private final UUID proxyId;
  private final QueueInfo queue;
  private final String pushFormat;
  private final EntityProperties properties;
  private final QueueStats queueStats;

  /**
   * @param queue pipeline handler key
   * @param pushFormat format parameter passed to the API endpoint.
   * @param proxyAPI handles interaction with Wavefront servers as well as queueing.
   * @param proxyId proxy ID.
   * @param properties container for mutable proxy settings.
   */
  LineDelimitedSenderTask(
      QueueInfo queue,
      int idx,
      String pushFormat,
      ProxyV2API proxyAPI,
      UUID proxyId,
      final EntityProperties properties,
      Buffer buffer,
      QueueStats queueStats) {
    super(queue, idx, properties, buffer);
    this.queue = queue;
    this.pushFormat = pushFormat;
    this.proxyId = proxyId;
    this.proxyAPI = proxyAPI;
    this.properties = properties;
    this.queueStats = queueStats;
  }

  // TODO: review
  @Override
  public int processSingleBatch(List<String> batch) {
    LineDelimitedDataSubmissionTask task =
        new LineDelimitedDataSubmissionTask(
            proxyAPI, proxyId, properties, pushFormat, queue, batch, null, queueStats);
    return task.execute();
  }
}
