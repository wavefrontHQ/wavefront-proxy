package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.EventAPI;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.Event;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * A {@link DataSubmissionTask} that handles event payloads.
 *
 * @author vasily@wavefront.com
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "__CLASS")
public class EventDataSubmissionTask extends AbstractDataSubmissionTask<EventDataSubmissionTask> {
  private transient EventAPI api;
  private transient UUID proxyId;

  @JsonProperty
  private List<Event> events;

  @SuppressWarnings("unused")
  EventDataSubmissionTask() {
  }

  /**
   * @param api          API endpoint.
   * @param proxyId      Proxy identifier. Used to authenticate proxy with the API.
   * @param properties   entity-specific wrapper over mutable proxy settings' container.
   * @param backlog      task queue.
   * @param handle       Handle (usually port number) of the pipeline where the data came from.
   * @param events       Data payload.
   * @param timeProvider Time provider (in millis).
   */
  public EventDataSubmissionTask(EventAPI api, UUID proxyId, EntityProperties properties,
                                 TaskQueue<EventDataSubmissionTask> backlog, String handle,
                                 @Nonnull List<Event> events,
                                 @Nullable Supplier<Long> timeProvider) {
    super(properties, backlog, handle, ReportableEntityType.EVENT, timeProvider);
    this.api = api;
    this.proxyId = proxyId;
    this.events = new ArrayList<>(events);
  }

  @Override
  public Response doExecute() {
    return api.proxyEvents(proxyId, events);
  }

  public List<EventDataSubmissionTask> splitTask(int minSplitSize, int maxSplitSize) {
    if (events.size() > Math.max(1, minSplitSize)) {
      List<EventDataSubmissionTask> result = new ArrayList<>();
      int stride = Math.min(maxSplitSize, (int) Math.ceil((float) events.size() / 2.0));
      int endingIndex = 0;
      for (int startingIndex = 0; endingIndex < events.size() - 1; startingIndex += stride) {
        endingIndex = Math.min(events.size(), startingIndex + stride) - 1;
        result.add(new EventDataSubmissionTask(api, proxyId, properties, backlog, handle,
            events.subList(startingIndex, endingIndex + 1), timeProvider));
      }
      return result;
    }
    return ImmutableList.of(this);
  }

  public List<Event> payload() {
    return events;
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
    this.timeProvider = System::currentTimeMillis;
  }
}
