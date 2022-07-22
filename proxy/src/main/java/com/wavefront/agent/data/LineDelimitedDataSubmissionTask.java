package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.core.handlers.LineDelimitedUtils;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.api.ProxyV2API;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

/** A {@link DataSubmissionTask} that handles plaintext payloads in the newline-delimited format. */
public class LineDelimitedDataSubmissionTask
    extends AbstractDataSubmissionTask<LineDelimitedDataSubmissionTask> {

  @VisibleForTesting @JsonProperty protected List<String> payload;
  private final QueueStats queueStats;
  private final transient ProxyV2API api;
  private final transient UUID proxyId;
  @JsonProperty private String format;

  /**
   * @param api API endpoint
   * @param proxyId Proxy identifier. Used to authenticate proxy with the API.
   * @param properties entity-specific wrapper over mutable proxy settings' container.
   * @param format Data format (passed as an argument to the API)
   * @param queue Handle (usually port number) of the pipeline where the data came from.
   * @param payload Data payload
   * @param timeProvider Time provider (in millis)
   */
  public LineDelimitedDataSubmissionTask(
      ProxyV2API api,
      UUID proxyId,
      EntityProperties properties,
      String format,
      QueueInfo queue,
      @Nonnull List<String> payload,
      @Nullable Supplier<Long> timeProvider,
      QueueStats queueStats) {
    super(properties, queue, timeProvider, queueStats);
    this.api = api;
    this.proxyId = proxyId;
    this.format = format;
    this.payload = new ArrayList<>(payload);
    this.queueStats = queueStats;
  }

  @Override
  Response doExecute() {
    return api.proxyReport(proxyId, format, LineDelimitedUtils.joinPushData(payload));
  }

  @Override
  public int size() {
    return this.payload.size();
  }
}
