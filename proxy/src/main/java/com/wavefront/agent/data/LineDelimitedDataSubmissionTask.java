package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.handlers.LineDelimitedUtils;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.ProxyV2API;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static com.wavefront.agent.queueing.QueueController.parsePostingResponse;

/**
 * A {@link DataSubmissionTask} that handles plaintext payloads in the newline-delimited format.
 *
 * @author vasily@wavefront.com
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "__CLASS_KEY")
public class LineDelimitedDataSubmissionTask
    extends AbstractDataSubmissionTask<LineDelimitedDataSubmissionTask> {

  private transient ProxyV2API api;
  private transient UUID proxyId;

  @JsonProperty
  private final String format;
  @JsonProperty
  private final List<String> payload;

  /**
   * Create a new instance.
   *
   * @param api          API endpoint
   * @param proxyId      Proxy identifier. Used to authenticate proxy with the API.
   * @param format       Data format (passed as an argument to the API)
   * @param entityType   Entity type handled
   * @param handle       Handle (usually port number) of the pipeline where the data came from.
   * @param payload      Data payload
   * @param timeProvider Time provider (in millis)
   */
  public LineDelimitedDataSubmissionTask(ProxyV2API api, UUID proxyId, String format,
                                         ReportableEntityType entityType, String handle,
                                         List<String> payload,
                                         @Nullable Supplier<Long> timeProvider) {
    super(handle, entityType, timeProvider);
    this.api = api;
    this.proxyId = proxyId;
    this.format = format;
    this.payload = payload;
  }

  @Override
  public TaskResult doExecute(TaskQueueingDirective queueingContext,
      TaskQueue<LineDelimitedDataSubmissionTask> taskQueue) {
    try {
      TaskAction taskAction = parsePostingResponse(api.proxyReport(proxyId, format,
          LineDelimitedUtils.joinPushData(payload)), handle);
      switch (taskAction) {
        case NONE:
          Metrics.newCounter(new MetricName(entityType + "." + handle, "", "delivered")).
              inc(this.weight());
          return TaskResult.COMPLETE;
        case ERROR:
        case PUSHBACK:
          if (enqueuedTimeMillis == null) {
            Metrics.newCounter(new MetricName(entityType + "." + handle, "", "queued")).
                inc(this.weight());
          }
          break;
        case SPLIT:
          //Metrics.newCounter(new MetricName(entityType + "." + handle, "", "retried")).
          //    inc(this.weight());
          //break;
      }
    } catch (Exception e) {

    }
    return TaskResult.COMPLETE;
  }

  @Override
  public int weight() {
    return this.payload.size();
  }

  @Override
  public List<LineDelimitedDataSubmissionTask> splitTask(int minSplitSize) {
    if (this.payload.size() > Math.max(1, minSplitSize)) {
      // in this case, split the payload in 2 batches approximately in the middle.
      int splitAt = this.payload.size() / 2;
      return ImmutableList.of(
          new LineDelimitedDataSubmissionTask(api, proxyId, format, getEntityType(), handle,
              payload.subList(0, splitAt), timeProvider),
          new LineDelimitedDataSubmissionTask(api, proxyId, format, getEntityType(), handle,
              payload.subList(splitAt + 1, payload.size()), timeProvider));
    }
    return ImmutableList.of(this);
  }

  public void injectMembers(ProxyV2API api, UUID proxyId) {
    this.api = api;
    this.proxyId = proxyId;
  }
}
