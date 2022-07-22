package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.api.LogAPI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

/**
 * A {@link DataSubmissionTask} that handles log payloads.
 *
 * @author amitw@vmware.com
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "__CLASS")
public class LogDataSubmissionTask extends AbstractDataSubmissionTask<LogDataSubmissionTask> {
  public static final String AGENT_PREFIX = "WF-PROXY-AGENT-";
  private final transient LogAPI api;
  private final transient UUID proxyId;

  @JsonProperty private List<String> logs;
  private int weight;

  /**
   * @param api API endpoint.
   * @param proxyId Proxy identifier
   * @param properties entity-specific wrapper over mutable proxy settings' container.
   * @param handle Handle (usually port number) of the pipeline where the data came from.
   * @param logs Data payload.
   * @param timeProvider Time provider (in millis).
   */
  public LogDataSubmissionTask(
      LogAPI api,
      UUID proxyId,
      EntityProperties properties,
      QueueInfo handle,
      @Nonnull List<String> logs,
      @Nullable Supplier<Long> timeProvider,
      QueueStats queueStats) {
    super(properties, handle, timeProvider, queueStats);
    this.api = api;
    this.proxyId = proxyId;
    this.logs = new ArrayList<>(logs); // TODO: review why?
    for (String l : logs) {
      weight += l.length();
    }
  }

  @Override
  Response doExecute() {
    return api.proxyLogsStr(AGENT_PREFIX + proxyId.toString(), "[" + String.join(",", logs) + "]");
  }

  @Override
  public int size() {
    return weight;
  }
}
