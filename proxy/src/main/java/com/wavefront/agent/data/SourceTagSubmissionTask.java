package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.dto.SourceTag;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

/** A {@link DataSubmissionTask} that handles source tag payloads. */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "__CLASS")
public class SourceTagSubmissionTask extends AbstractDataSubmissionTask<SourceTagSubmissionTask> {
  private transient SourceTagAPI api;

  @JsonProperty private SourceTag sourceTag;

  /**
   * @param api API endpoint.
   * @param properties container for mutable proxy settings.
   * @param handle Handle (usually port number) of the pipeline where the data came from.
   * @param sourceTag source tag operation
   * @param timeProvider Time provider (in millis).
   */
  public SourceTagSubmissionTask(
      SourceTagAPI api,
      EntityProperties properties,
      QueueInfo handle,
      @Nonnull SourceTag sourceTag,
      @Nullable Supplier<Long> timeProvider,
      QueueStats queueStats) {
    super(properties, handle, timeProvider, queueStats);
    this.api = api;
    this.sourceTag = sourceTag;
    this.limitRetries = true;
  }

  @Nullable
  Response doExecute() throws DataSubmissionException {
    switch (sourceTag.getOperation()) {
      case SOURCE_DESCRIPTION:
        switch (sourceTag.getAction()) {
          case DELETE:
            Response resp = api.removeDescription(sourceTag.getSource());
            if (resp.getStatus() == 404) {
              throw new IgnoreStatusCodeException(
                  "Attempting to delete description for "
                      + "a non-existent source  "
                      + sourceTag.getSource()
                      + ", ignoring");
            }
            return resp;
          case SAVE:
          case ADD:
            return api.setDescription(sourceTag.getSource(), sourceTag.getAnnotations().get(0));
          default:
            throw new IllegalArgumentException("Invalid acton: " + sourceTag.getAction());
        }
      case SOURCE_TAG:
        switch (sourceTag.getAction()) {
          case ADD:
            String addTag = sourceTag.getAnnotations().get(0);
            Response re = api.appendTag(sourceTag.getSource(), addTag);
            if (re.getStatus() == 404) {
              throw new IgnoreStatusCodeException(
                  "Failed to add tag "
                      + addTag
                      + " for source "
                      + sourceTag.getSource()
                      + ", ignoring");
            }
            return re;
          case DELETE:
            String tag = sourceTag.getAnnotations().get(0);
            Response resp = api.removeTag(sourceTag.getSource(), tag);
            if (resp.getStatus() == 404) {
              throw new IgnoreStatusCodeException(
                  "Attempting to delete non-existing tag "
                      + tag
                      + " for source "
                      + sourceTag.getSource()
                      + ", ignoring");
            }
            return resp;
          case SAVE:
            return api.setTags(sourceTag.getSource(), sourceTag.getAnnotations());
          default:
            throw new IllegalArgumentException("Invalid acton: " + sourceTag.getAction());
        }
      default:
        throw new IllegalArgumentException(
            "Invalid source tag operation: " + sourceTag.getOperation());
    }
  }

  public SourceTag payload() {
    return sourceTag;
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public List<SourceTagSubmissionTask> splitTask(int minSplitSize, int maxSplitSize) {
    return ImmutableList.of(this);
  }
}
