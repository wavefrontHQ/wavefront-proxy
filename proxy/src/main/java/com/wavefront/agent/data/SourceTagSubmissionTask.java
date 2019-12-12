package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.data.ReportableEntityType;
import wavefront.report.ReportSourceTag;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.function.Supplier;

import static com.wavefront.ingester.ReportSourceTagIngesterFormatter.ACTION_ADD;
import static com.wavefront.ingester.ReportSourceTagIngesterFormatter.ACTION_DELETE;
import static com.wavefront.ingester.ReportSourceTagIngesterFormatter.ACTION_SAVE;

/**
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "__CLASS_KEY")
public class SourceTagSubmissionTask extends AbstractDataSubmissionTask<SourceTagSubmissionTask> {
  private transient SourceTagAPI api;

  @JsonProperty
  private final ReportSourceTag sourceTag;

  public SourceTagSubmissionTask(SourceTagAPI api, String handle,
                                 ReportSourceTag sourceTag,
                                 @Nullable Supplier<Long> timeProvider) {
    super(handle, ReportableEntityType.SOURCE_TAG, timeProvider);
    this.api = api;
    this.sourceTag = sourceTag;
  }

  @Nullable
  TaskResult doExecute(TaskQueueingDirective queueingContext,
                       TaskQueue<SourceTagSubmissionTask> taskQueue) {
    Response response = null;
    try {
      switch (sourceTag.getSourceTagLiteral()) {
        case "SourceDescription":
          if (sourceTag.getAction().equals("delete")) {
            response = api.removeDescription(sourceTag.getSource());
          } else {
            response = api.setDescription(sourceTag.getSource(), sourceTag.getDescription());
          }
        case "SourceTag":
          switch (sourceTag.getAction()) {
            case ACTION_ADD:
              response = api.appendTag(sourceTag.getSource(), sourceTag.getAnnotations().get(0));
            case ACTION_DELETE:
              response = api.removeTag(sourceTag.getSource(), sourceTag.getAnnotations().get(0));
            case ACTION_SAVE:
              response = api.setTags(sourceTag.getSource(), sourceTag.getAnnotations());
          }
      }
    } catch (Exception e) {
      // TODO handle
    }
    return TaskResult.COMPLETE;
  }

  @Override
  public int weight() {
    return 1;
  }

  @Override
  public ReportableEntityType getEntityType() {
    return ReportableEntityType.SOURCE_TAG;
  }

  @Override
  public List<SourceTagSubmissionTask> splitTask(int minSplitSize) {
    return ImmutableList.of(this);
  }

  public void injectMembers(SourceTagAPI api) {
    this.api = api;
  }
}
