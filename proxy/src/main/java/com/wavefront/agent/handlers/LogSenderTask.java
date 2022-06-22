package com.wavefront.agent.handlers;

import com.wavefront.agent.buffer.QueueInfo;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.LogDataSubmissionTask;
import com.wavefront.api.LogAPI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

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
  private final ScheduledExecutorService scheduler;

  /**
   * @param handlerKey handler key, that serves as an identifier of the log pipeline.
   * @param logAPI handles interaction with log systems as well as queueing.
   * @param proxyId id of the proxy.
   * @param threadId thread number.
   * @param properties container for mutable proxy settings.
   * @param scheduler executor service for running this task
   */
  LogSenderTask(
      QueueInfo handlerKey,
      LogAPI logAPI,
      UUID proxyId,
      EntityProperties properties,
      ScheduledExecutorService scheduler) {
    super(handlerKey, properties, scheduler);
    this.queue = handlerKey;
    this.logAPI = logAPI;
    this.proxyId = proxyId;
    this.properties = properties;
    this.scheduler = scheduler;
  }

  // TODO: review
  @Override
  public int processSingleBatch(List<String> batch) {
    LogDataSubmissionTask task =
        new LogDataSubmissionTask(logAPI, proxyId, properties, queue, batch, null);
    return task.execute();
  }

  //  @Override
  //  public void flushSingleBatch(List<Log> batch, @Nullable QueueingReason reason) {
  //    LogDataSubmissionTask task = new LogDataSubmissionTask(logAPI, proxyId, properties,
  //        backlog, handlerKey.getHandle(), batch, null);
  //    task.enqueue(reason);
  //  }
}
