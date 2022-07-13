package com.wavefront.agent.core.senders;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.api.EventAPI;
import java.util.List;
import java.util.UUID;

/**
 * This class is responsible for accumulating events and sending them batch. This class is similar
 * to PostPushDataTimedTask.
 *
 * @author vasily@wavefront.com
 */
class EventSenderTask extends AbstractSenderTask {

  private final QueueInfo queue;
  private final int idx;
  private final EventAPI proxyAPI;
  private final UUID proxyId;
  private final EntityProperties properties;
  private final Buffer buffer;
  private SenderStats senderStats;

  /**
   * @param queue handler key, that serves as an identifier of the metrics pipeline.
   * @param proxyAPI handles interaction with Wavefront servers as well as queueing.
   * @param proxyId id of the proxy.
   * @param properties container for mutable proxy settings.
   * @param senderStats
   */
  EventSenderTask(
      QueueInfo queue,
      int idx,
      EventAPI proxyAPI,
      UUID proxyId,
      EntityProperties properties,
      Buffer buffer,
      SenderStats senderStats) {
    super(queue, idx, properties, buffer);
    this.queue = queue;
    this.idx = idx;
    this.proxyAPI = proxyAPI;
    this.proxyId = proxyId;
    this.properties = properties;
    this.buffer = buffer;
    this.senderStats = senderStats;
  }

  // TODO: review

  //  @Override
  //  TaskResult processSingleBatch(List<Event> batch) {
  //    EventDataSubmissionTask task = new EventDataSubmissionTask(proxyAPI, proxyId, properties,
  //        backlog, handlerKey.getHandle(), batch, null);
  //    return task.execute();
  //  }
  //
  //  @Override
  //  public void flushSingleBatch(List<Event> batch, @Nullable QueueingReason reason) {
  //    EventDataSubmissionTask task = new EventDataSubmissionTask(proxyAPI, proxyId, properties,
  //        backlog, handlerKey.getHandle(), batch, null);
  //    task.enqueue(reason);
  //  }

  @Override
  public int processSingleBatch(List<String> batch) {
    EventDataSubmissionTask task =
        new EventDataSubmissionTask(proxyAPI, proxyId, properties, queue, batch, null, senderStats);
    return task.execute();
  }
}
