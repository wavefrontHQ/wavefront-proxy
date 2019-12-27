package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.data.EntityWrapper.EntityProperties;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.EventAPI;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.Event;
import wavefront.report.ReportEvent;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This class is responsible for accumulating events and sending them batch. This
 * class is similar to PostPushDataTimedTask.
 *
 * @author vasily@wavefront.com
 */
class EventSenderTask extends AbstractSenderTask<ReportEvent> {
  private static final Logger logger = Logger.getLogger(EventSenderTask.class.getCanonicalName());

  private final EventAPI proxyAPI;
  private final UUID proxyId;
  private final TaskQueue<EventDataSubmissionTask> backlog;

  /**
   * @param proxyAPI     handles interaction with Wavefront servers as well as queueing.
   * @param proxyId      id of the proxy.
   * @param handle       handle (usually port number), that serves as an identifier for the metrics pipeline.
   * @param threadId     thread number.
   * @param properties   container for mutable proxy settings.
   * @param rateLimiter  rate limiter to control outbound point rate.
   * @param backlog      backing queue
   */
  EventSenderTask(EventAPI proxyAPI, UUID proxyId, String handle, int threadId,
                  EntityProperties properties, @Nullable RecyclableRateLimiter rateLimiter,
                  TaskQueue<EventDataSubmissionTask> backlog) {
    super(ReportableEntityType.EVENT, handle, threadId, properties, rateLimiter);
    this.proxyAPI = proxyAPI;
    this.proxyId = proxyId;
    this.backlog = backlog;
    this.scheduler.schedule(this, properties.getPushFlushInterval(), TimeUnit.MILLISECONDS);
  }

  @Override
  TaskResult processSingleBatch(List<ReportEvent> batch) {
    EventDataSubmissionTask task = new EventDataSubmissionTask(proxyAPI, proxyId, properties,
        backlog, handle, batch.stream().map(Event::new).collect(Collectors.toList()), null);
    return task.execute();
  }

  @Override
  public void flushSingleBatch(List<ReportEvent> batch, QueueingReason reason) {
    EventDataSubmissionTask task = new EventDataSubmissionTask(proxyAPI, proxyId, properties,
        backlog, handle, batch.stream().map(Event::new).collect(Collectors.toList()), null);
    task.enqueue(reason);
  }
}
