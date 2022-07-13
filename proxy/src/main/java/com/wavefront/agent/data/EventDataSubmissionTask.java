package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.senders.SenderStats;
import com.wavefront.api.EventAPI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

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

  @JsonProperty private List<String> events;
  private SenderStats senderStats;

  /**
   * @param api API endpoint.
   * @param proxyId Proxy identifier. Used to authenticate proxy with the API.
   * @param properties entity-specific wrapper over mutable proxy settings' container.
   * @param queue Handle (usually port number) of the pipeline where the data came from.
   * @param events Data payload.
   * @param timeProvider Time provider (in millis).
   */
  public EventDataSubmissionTask(
      EventAPI api,
      UUID proxyId,
      EntityProperties properties,
      QueueInfo queue,
      @Nonnull List<String> events,
      @Nullable Supplier<Long> timeProvider,
      SenderStats senderStats) {
    super(properties, queue, timeProvider, senderStats);
    this.api = api;
    this.proxyId = proxyId;
    this.events = events;
    this.senderStats = senderStats;
  }

  @Override
  public Response doExecute() {
    return api.proxyEventsString(proxyId, "[" + String.join(",", events) + "]");
  }

  public List<EventDataSubmissionTask> splitTask(int minSplitSize, int maxSplitSize) {
    if (events.size() > Math.max(1, minSplitSize)) {
      List<EventDataSubmissionTask> result = new ArrayList<>();
      int stride = Math.min(maxSplitSize, (int) Math.ceil((float) events.size() / 2.0));
      int endingIndex = 0;
      for (int startingIndex = 0; endingIndex < events.size() - 1; startingIndex += stride) {
        endingIndex = Math.min(events.size(), startingIndex + stride) - 1;
        result.add(
            new EventDataSubmissionTask(
                api,
                proxyId,
                properties,
                queue,
                events.subList(startingIndex, endingIndex + 1),
                timeProvider,
                senderStats));
      }
      return result;
    }
    return ImmutableList.of(this);
  }

  @Override
  public int size() {
    return events.size();
  }
}
