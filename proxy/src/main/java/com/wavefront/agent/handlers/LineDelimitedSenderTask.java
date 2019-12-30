package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.data.EntityWrapper.EntityProperties;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.agent.queueing.TaskSizeEstimator;
import com.wavefront.api.ProxyV2API;
import com.wavefront.data.ReportableEntityType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * SenderTask for newline-delimited data.
 *
 * @author vasily@wavefront.com
 */
class LineDelimitedSenderTask extends AbstractSenderTask<String> {
  private static final Logger logger =
      Logger.getLogger(LineDelimitedSenderTask.class.getCanonicalName());

  private final ProxyV2API proxyAPI;
  private final UUID proxyId;
  private final String pushFormat;
  private final TaskSizeEstimator taskSizeEstimator;
  private final TaskQueue<LineDelimitedDataSubmissionTask> backlog;

  /**
   * @param entityType        entity type that dictates the data processing flow.
   * @param pushFormat        format parameter passed to the API endpoint.
   * @param proxyAPI          handles interaction with Wavefront servers as well as queueing.
   * @param proxyId           proxy ID.
   * @param handle            handle (usually port number), that serves as an identifier for the
   *                          metrics pipeline.
   * @param properties        container for mutable proxy settings.
   * @param threadId          thread number.
   * @param rateLimiter       rate limiter to control outbound point rate.
   * @param taskSizeEstimator optional task size estimator used to calculate approximate
   *                          buffer fill rate.
   * @param backlog           backing queue.
   */
  LineDelimitedSenderTask(ReportableEntityType entityType, String pushFormat,
                          ProxyV2API proxyAPI, UUID proxyId, String handle,
                          final EntityProperties properties,
                          int threadId, final RecyclableRateLimiter rateLimiter,
                          @Nullable final TaskSizeEstimator taskSizeEstimator,
                          TaskQueue<LineDelimitedDataSubmissionTask> backlog) {
    super(entityType, handle, threadId, properties, rateLimiter);
    this.pushFormat = pushFormat;
    this.proxyId = proxyId;
    this.proxyAPI = proxyAPI;
    this.taskSizeEstimator = taskSizeEstimator;
    this.backlog = backlog;
    this.scheduler.schedule(this, properties.getPushFlushInterval(), TimeUnit.MILLISECONDS);
  }

  @Override
  TaskResult processSingleBatch(List<String> batch) {
    LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(proxyAPI,
        proxyId, properties, backlog, pushFormat, entityType, handle, batch, null);
    if (taskSizeEstimator != null) taskSizeEstimator.scheduleTaskForSizing(task);
    return task.execute();
  }

  @Override
  void flushSingleBatch(List<String> batch, QueueingReason reason) {
    LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(proxyAPI,
        proxyId, properties, backlog, pushFormat, entityType, handle, batch, null);
    task.enqueue(reason);
  }
}
