package com.wavefront.agent.handlers;

import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.api.EventAPI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This class is responsible for accumulating events and sending them batch. This class is similar
 * to PostPushDataTimedTask.
 *
 * @author vasily@wavefront.com
 */
class EventSenderTask extends AbstractSenderTask {

  private final EventAPI proxyAPI;
  private final UUID proxyId;

  /**
   * @param handlerKey handler key, that serves as an identifier of the metrics pipeline.
   * @param proxyAPI handles interaction with Wavefront servers as well as queueing.
   * @param proxyId id of the proxy.
   * @param threadId thread number.
   * @param properties container for mutable proxy settings.
   * @param scheduler executor service for running this task
   */
  EventSenderTask(
      HandlerKey handlerKey,
      EventAPI proxyAPI,
      UUID proxyId,
      int threadId,
      EntityProperties properties,
      ScheduledExecutorService scheduler) {
    super(handlerKey, threadId, properties, scheduler);
    this.proxyAPI = proxyAPI;
    this.proxyId = proxyId;
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
  TaskResult processSingleBatch(List<String> batch) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
