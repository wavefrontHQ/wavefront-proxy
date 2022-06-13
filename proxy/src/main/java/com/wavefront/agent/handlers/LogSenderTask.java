package com.wavefront.agent.handlers;

import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.LogDataSubmissionTask;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.LogAPI;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This class is responsible for accumulating logs and uploading them in batches.
 *
 * @author amitw@vmware.com
 */
public class LogSenderTask extends AbstractSenderTask {
  private final LogAPI logAPI;
  private final UUID proxyId;
  private final TaskQueue<LogDataSubmissionTask> backlog;

  /**
   * @param handlerKey handler key, that serves as an identifier of the log pipeline.
   * @param logAPI handles interaction with log systems as well as queueing.
   * @param proxyId id of the proxy.
   * @param threadId thread number.
   * @param properties container for mutable proxy settings.
   * @param scheduler executor service for running this task
   * @param backlog backing queue
   */
  LogSenderTask(
      HandlerKey handlerKey,
      LogAPI logAPI,
      UUID proxyId,
      int threadId,
      EntityProperties properties,
      ScheduledExecutorService scheduler,
      TaskQueue<LogDataSubmissionTask> backlog) {
    super(handlerKey, threadId, properties, scheduler);
    this.logAPI = logAPI;
    this.proxyId = proxyId;
    this.backlog = backlog;
  }

  // TODO: review
  //  @Override
  //  TaskResult processSingleBatch(List<Log> batch) {
  //    LogDataSubmissionTask task = new LogDataSubmissionTask(logAPI, proxyId, properties,
  //        backlog, handlerKey.getHandle(), batch, null);
  //    return task.execute();
  //  }
  //
  //  @Override
  //  public void flushSingleBatch(List<Log> batch, @Nullable QueueingReason reason) {
  //    LogDataSubmissionTask task = new LogDataSubmissionTask(logAPI, proxyId, properties,
  //        backlog, handlerKey.getHandle(), batch, null);
  //    task.enqueue(reason);
  //  }
}
