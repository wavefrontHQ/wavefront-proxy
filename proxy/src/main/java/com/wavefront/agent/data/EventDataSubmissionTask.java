package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.data.EntityWrapper.EntityProperties;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.EventAPI;
import com.wavefront.api.ProxyV2API;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.Event;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * TODO (VV): javadoc
 *
 * @author vasily@wavefront.com
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "__CLASS")
public class EventDataSubmissionTask extends AbstractDataSubmissionTask<EventDataSubmissionTask> {
  private transient EventAPI api;
  private transient UUID proxyId;

  private List<Event> events;

  @SuppressWarnings("unused")
  EventDataSubmissionTask() {
  }

  /**
   * TODO (VV): javadoc
   *
   * @param api
   * @param proxyId
   * @param handle
   * @param events
   * @param timeProvider
   */
  public EventDataSubmissionTask(EventAPI api, UUID proxyId, EntityProperties properties,
                                 TaskQueue<EventDataSubmissionTask> backlog, String handle,
                                 List<Event> events, @Nullable Supplier<Long> timeProvider) {
    super(properties, backlog, handle, ReportableEntityType.EVENT, timeProvider);
    this.api = api;
    this.proxyId = proxyId;
    this.events = events;
  }

  @Override
  public Response doExecute() {
    return api.proxyEvents(proxyId, events);
  }

  @Override
  public List<EventDataSubmissionTask> splitTask(int minSplitSize, int maxSplitSize) {
    // TODO (VV): implement
    return ImmutableList.of(this);
  }

  @Override
  public int weight() {
    return events.size();
  }

  public void injectMembers(EventAPI api, UUID proxyId, EntityProperties properties,
                            TaskQueue<EventDataSubmissionTask> backlog) {
    this.api = api;
    this.proxyId = proxyId;
    this.properties = properties;
    this.backlog = backlog;
  }
}
