package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.api.EventAPI;
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
  private final transient EventAPI api;
  private final transient UUID proxyId;

  @JsonProperty private List<String> events;
  private final QueueStats queueStats;

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
      QueueStats queueStats) {
    super(properties, queue, timeProvider, queueStats);
    this.api = api;
    this.proxyId = proxyId;
    this.events = events;
    this.queueStats = queueStats;
  }

  @Override
  public Response doExecute() {
    return api.proxyEventsString(proxyId, "[" + String.join(",", events) + "]");
  }

  @Override
  public int size() {
    return events.size();
  }
}
