package com.wavefront.agent.handlers;

import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.EventAPI;
import com.wavefront.dto.Event;
import wavefront.report.ReportEvent;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class is responsible for accumulating events and sending them batch. This
 * class is similar to PostPushDataTimedTask.
 *
 * @author vasily@wavefront.com
 */
class EventSenderTask extends AbstractSenderTask<ReportEvent> {

  private final EventAPI proxyAPI;
  private final UUID proxyId;
  private final TaskQueue<EventDataSubmissionTask> backlog;

  /**
   * @param handlerKey   handler key, that serves as an identifier of the metrics pipeline.
   * @param proxyAPI     handles interaction with Wavefront servers as well as queueing.
   * @param proxyId      id of the proxy.
   * @param threadId     thread number.
   * @param properties   container for mutable proxy settings.
   * @param backlog      backing queue
   */
  EventSenderTask(HandlerKey handlerKey, EventAPI proxyAPI, UUID proxyId, int threadId,
                  EntityProperties properties, TaskQueue<EventDataSubmissionTask> backlog) {
    super(handlerKey, threadId, properties);
    this.proxyAPI = proxyAPI;
    this.proxyId = proxyId;
    this.backlog = backlog;
    this.scheduler.schedule(this, properties.getPushFlushInterval(), TimeUnit.MILLISECONDS);
  }

  @Override
  TaskResult processSingleBatch(List<ReportEvent> batch) {
    EventDataSubmissionTask task = new EventDataSubmissionTask(proxyAPI, proxyId, properties,
        backlog, handlerKey.getHandle(),
        batch.stream().map(Event::new).collect(Collectors.toList()), null);
    return task.execute();
  }

  @Override
  public void flushSingleBatch(List<ReportEvent> batch, @Nullable QueueingReason reason) {
    EventDataSubmissionTask task = new EventDataSubmissionTask(proxyAPI, proxyId, properties,
        backlog, handlerKey.getHandle(),
        batch.stream().map(Event::new).collect(Collectors.toList()), null);
    task.enqueue(reason);
  }
}
