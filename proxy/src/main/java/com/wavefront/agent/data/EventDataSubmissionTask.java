package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.EventAPI;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.Event;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 *
 *
 * @author vasily@wavefront.com
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "__CLASS_KEY")
public class EventDataSubmissionTask extends AbstractDataSubmissionTask<EventDataSubmissionTask> {
  private transient EventAPI api;
  private transient UUID proxyId;

  private final List<Event> events;

  /**
   *
   *
   * @param api           TODO
   * @param proxyId       TODO
   * @param handle        TODO
   * @param events        TODO
   * @param timeProvider  TODO
   */
  public EventDataSubmissionTask(EventAPI api, UUID proxyId, String handle, List<Event> events,
                                 @Nullable Supplier<Long> timeProvider) {
    super(handle, ReportableEntityType.EVENT, timeProvider);
    this.api = api;
    this.proxyId = proxyId;
    this.events = events;
  }

  @Override
  public TaskResult doExecute(TaskQueueingDirective queueingContext,
                              TaskQueue<EventDataSubmissionTask> taskQueue) {
    api.proxyEvents(proxyId, events);
    // TODO: Return correct status
    return TaskResult.COMPLETE;
  }

  @Override
  public List<EventDataSubmissionTask> splitTask(int minSplitSize) {
    return ImmutableList.of(this);
  }

  @Override
  public int weight() {
    return 1;
  }

  public void injectMembers(EventAPI api, UUID proxyId) {
    this.api = api;
    this.proxyId = proxyId;
  }
}
