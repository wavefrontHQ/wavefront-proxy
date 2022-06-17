package com.wavefront.agent.handlers;

import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.api.ProxyV2API;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

/**
 * SenderTask for newline-delimited data.
 *
 * @author vasily@wavefront.com
 */
class LineDelimitedSenderTask extends AbstractSenderTask {

  private final ProxyV2API proxyAPI;
  private final UUID proxyId;
  private final HandlerKey handlerKey;
  private final String pushFormat;
  private EntityProperties properties;
  private final ScheduledExecutorService scheduler;
  private final int threadId;

  /**
   * @param handlerKey pipeline handler key
   * @param pushFormat format parameter passed to the API endpoint.
   * @param proxyAPI handles interaction with Wavefront servers as well as queueing.
   * @param proxyId proxy ID.
   * @param properties container for mutable proxy settings.
   * @param scheduler executor service for running this task
   * @param threadId thread number.
   */
  LineDelimitedSenderTask(
      HandlerKey handlerKey,
      String pushFormat,
      ProxyV2API proxyAPI,
      UUID proxyId,
      final EntityProperties properties,
      ScheduledExecutorService scheduler,
      int threadId) {
    super(handlerKey, threadId, properties, scheduler);
    this.handlerKey = handlerKey;
    this.pushFormat = pushFormat;
    this.proxyId = proxyId;
    this.proxyAPI = proxyAPI;
    this.properties = properties;
    this.scheduler = scheduler;
    this.threadId = threadId;
  }

  // TODO: review
  @Override
  TaskResult processSingleBatch(List<String> batch) {
    LineDelimitedDataSubmissionTask task =
        new LineDelimitedDataSubmissionTask(
            proxyAPI, proxyId, properties, pushFormat, handlerKey, batch, null);
    return task.execute();
  }

  //  @Override
  //  void flushSingleBatch(List<String> batch, @Nullable QueueingReason reason) {
  //    LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(proxyAPI,
  //        proxyId, properties, backlog, pushFormat, handlerKey.getEntityType(),
  //        handlerKey.getHandle(), batch, null);
  //    task.enqueue(reason);
  //  }
}
