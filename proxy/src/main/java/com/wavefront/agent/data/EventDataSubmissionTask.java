package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.EventAPI;
import com.wavefront.api.ProxyV2API;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.EventDTO;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.io.IOException;
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

  private final EventDTO eventDTO;

  /**
   *
   *
   * @param api
   * @param handle
   * @param eventDTO
   * @param timeProvider
   */
  public EventDataSubmissionTask(EventAPI api, String handle, EventDTO eventDTO,
                                 @Nullable Supplier<Long> timeProvider) {
    super(handle, ReportableEntityType.EVENT, timeProvider);
    this.api = api;
    this.eventDTO = eventDTO;
  }

  @Override
  public TaskResult doExecute(TaskQueueingDirective queueingContext,
                              TaskQueue<EventDataSubmissionTask> taskQueue) {
    api.createEvent(eventDTO);
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

  public void injectMembers(EventAPI api) {
    this.api = api;
  }
}
