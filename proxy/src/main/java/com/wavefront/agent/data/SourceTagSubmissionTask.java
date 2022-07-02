package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.dto.SourceTag;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

/**
 * A {@link DataSubmissionTask} that handles source tag payloads.
 *
 * @author vasily@wavefront.com
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "__CLASS")
public class SourceTagSubmissionTask extends AbstractDataSubmissionTask<SourceTagSubmissionTask> {
  private transient SourceTagAPI api;

  @JsonProperty private SourceTag sourceTag;

  @SuppressWarnings("unused")
  SourceTagSubmissionTask() {}

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
      com.wavefront.agent.core.queues.QueueInfo handle,
      @Nonnull SourceTag sourceTag,
      @Nullable Supplier<Long> timeProvider) {
    super(properties, handle, timeProvider);
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
            return api.appendTag(sourceTag.getSource(), sourceTag.getAnnotations().get(0));
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
  public int weight() {
    return 1;
  }

  @Override
  public List<SourceTagSubmissionTask> splitTask(int minSplitSize, int maxSplitSize) {
    return ImmutableList.of(this);
  }

  public void injectMembers(SourceTagAPI api, EntityProperties properties) {
    this.api = api;
    this.properties = properties;
    this.timeProvider = System::currentTimeMillis;
  }
}
