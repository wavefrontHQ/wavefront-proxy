package com.wavefront.agent.handlers;

import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.agent.queueing.TaskSizeEstimator;
import com.wavefront.api.ProxyV2API;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

/**
 * SenderTask for newline-delimited data.
 *
 * @author vasily@wavefront.com
 */
class LineDelimitedSenderTask extends AbstractSenderTask<String> {

  private final ProxyV2API proxyAPI;
  private final UUID proxyId;
  private final String pushFormat;
  private final TaskSizeEstimator taskSizeEstimator;
  private final TaskQueue<LineDelimitedDataSubmissionTask> backlog;

  /**
   * @param handlerKey        pipeline handler key
   * @param pushFormat        format parameter passed to the API endpoint.
   * @param proxyAPI          handles interaction with Wavefront servers as well as queueing.
   * @param proxyId           proxy ID.
   * @param properties        container for mutable proxy settings.
   * @param scheduler         executor service for running this task
   * @param threadId          thread number.
   * @param taskSizeEstimator optional task size estimator used to calculate approximate
   *                          buffer fill rate.
   * @param backlog           backing queue.
   */
  LineDelimitedSenderTask(HandlerKey handlerKey, String pushFormat, ProxyV2API proxyAPI,
                          UUID proxyId, final EntityProperties properties,
                          ScheduledExecutorService scheduler, int threadId,
                          @Nullable final TaskSizeEstimator taskSizeEstimator,
                          TaskQueue<LineDelimitedDataSubmissionTask> backlog) {
    super(handlerKey, threadId, properties, scheduler);
    this.pushFormat = pushFormat;
    this.proxyId = proxyId;
    this.proxyAPI = proxyAPI;
    this.taskSizeEstimator = taskSizeEstimator;
    this.backlog = backlog;
  }

  @Override
  TaskResult processSingleBatch(List<String> batch) {
    LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(proxyAPI,
        proxyId, properties, backlog, pushFormat, handlerKey.getEntityType(),
        handlerKey.getHandle(), batch, null);
    if (taskSizeEstimator != null) taskSizeEstimator.scheduleTaskForSizing(task);
    return task.execute();
  }

  @Override
  void flushSingleBatch(List<String> batch, @Nullable QueueingReason reason) {
    LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(proxyAPI,
        proxyId, properties, backlog, pushFormat, handlerKey.getEntityType(),
        handlerKey.getHandle(), batch, null);
    task.enqueue(reason);
  }
}
