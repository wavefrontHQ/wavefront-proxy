package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.core.handlers.LineDelimitedUtils;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.api.ProxyV2API;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

/**
 * A {@link DataSubmissionTask} that handles plaintext payloads in the newline-delimited format.
 *
 * @author vasily@wavefront.com
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "__CLASS")
public class LineDelimitedDataSubmissionTask
    extends AbstractDataSubmissionTask<LineDelimitedDataSubmissionTask> {

  @VisibleForTesting @JsonProperty protected List<String> payload;
  private transient ProxyV2API api;
  private transient UUID proxyId;
  @JsonProperty private String format;

  @SuppressWarnings("unused")
  LineDelimitedDataSubmissionTask() {}

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
      @Nullable Supplier<Long> timeProvider) {
    super(properties, queue, timeProvider);
    this.api = api;
    this.proxyId = proxyId;
    this.format = format;
    this.payload = new ArrayList<>(payload);
  }

  @Override
  Response doExecute() {
    return api.proxyReport(proxyId, format, LineDelimitedUtils.joinPushData(payload));
  }

  @Override
  public int weight() {
    return this.payload.size();
  }

  @Override
  public List<LineDelimitedDataSubmissionTask> splitTask(int minSplitSize, int maxSplitSize) {
    if (payload.size() > Math.max(1, minSplitSize)) {
      List<LineDelimitedDataSubmissionTask> result = new ArrayList<>();
      int stride = Math.min(maxSplitSize, (int) Math.ceil((float) payload.size() / 2.0));
      int endingIndex = 0;
      for (int startingIndex = 0; endingIndex < payload.size() - 1; startingIndex += stride) {
        endingIndex = Math.min(payload.size(), startingIndex + stride) - 1;
        result.add(
            new LineDelimitedDataSubmissionTask(
                api,
                proxyId,
                properties,
                format,
                queue,
                payload.subList(startingIndex, endingIndex + 1),
                timeProvider));
      }
      return result;
    }
    return ImmutableList.of(this);
  }

  public List<String> payload() {
    return payload;
  }

  public void injectMembers(ProxyV2API api, UUID proxyId, EntityProperties properties) {
    this.api = api;
    this.proxyId = proxyId;
    this.properties = properties;
    this.timeProvider = System::currentTimeMillis;
  }
}
