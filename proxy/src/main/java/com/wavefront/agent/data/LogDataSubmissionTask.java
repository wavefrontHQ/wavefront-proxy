package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.api.LogAPI;
import com.wavefront.dto.Log;
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
  private transient LogAPI api;
  private transient UUID proxyId;

  @JsonProperty private List<Log> logs;
  private int weight;

  @SuppressWarnings("unused")
  LogDataSubmissionTask() {}

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
      HandlerKey handle,
      @Nonnull List<Log> logs,
      @Nullable Supplier<Long> timeProvider) {
    super(properties, handle, timeProvider);
    this.api = api;
    this.proxyId = proxyId;
    this.logs = new ArrayList<>(logs);
    for (Log l : logs) {
      weight += l.getDataSize();
    }
  }

  @Override
  Response doExecute() {
    return api.proxyLogs(AGENT_PREFIX + proxyId.toString(), logs);
  }

  @Override
  public int weight() {
    return weight;
  }

  @Override
  public List<LogDataSubmissionTask> splitTask(int minSplitSize, int maxSplitSize) {
    if (logs.size() > Math.max(1, minSplitSize)) {
      List<LogDataSubmissionTask> result = new ArrayList<>();
      int stride = Math.min(maxSplitSize, (int) Math.ceil((float) logs.size() / 2.0));
      int endingIndex = 0;
      for (int startingIndex = 0; endingIndex < logs.size() - 1; startingIndex += stride) {
        endingIndex = Math.min(logs.size(), startingIndex + stride) - 1;
        result.add(
            new LogDataSubmissionTask(
                api,
                proxyId,
                properties,
                handle,
                logs.subList(startingIndex, endingIndex + 1),
                timeProvider));
      }
      return result;
    }
    return ImmutableList.of(this);
  }

  public void injectMembers(LogAPI api, UUID proxyId, EntityProperties properties) {
    this.api = api;
    this.proxyId = proxyId;
    this.properties = properties;
    this.timeProvider = System::currentTimeMillis;
  }
}
