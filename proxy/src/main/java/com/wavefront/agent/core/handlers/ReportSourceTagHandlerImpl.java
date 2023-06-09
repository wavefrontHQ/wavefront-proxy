package com.wavefront.agent.core.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.data.Validation;
import com.wavefront.dto.SourceTag;
import java.util.function.Function;
import org.slf4j.Logger;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;

/** This class will validate parsed source tags and distribute them among SenderTask threads. */
class ReportSourceTagHandlerImpl
    extends AbstractReportableEntityHandler<ReportSourceTag, SourceTag> {
  private static final Function<ReportSourceTag, String> SOURCE_TAG_SERIALIZER =
      value -> new SourceTag(value).toString();

  public ReportSourceTagHandlerImpl(
      String handler, QueueInfo handlerKey, final Logger blockedItemLogger) {
    super(handler, handlerKey, SOURCE_TAG_SERIALIZER, blockedItemLogger);
  }

  @VisibleForTesting
  static boolean annotationsAreValid(ReportSourceTag sourceTag) {
    if (sourceTag.getOperation() == SourceOperationType.SOURCE_DESCRIPTION) return true;
    return sourceTag.getAnnotations().stream().allMatch(Validation::charactersAreValid);
  }

  @Override
  protected void reportInternal(ReportSourceTag sourceTag) {
    if (!annotationsAreValid(sourceTag)) {
      throw new IllegalArgumentException(
          "WF-401: SourceTag annotation key has illegal characters.");
    }

    try {
      ObjectWriter ow = new ObjectMapper().writer();
      String json = ow.writeValueAsString(new SourceTag(sourceTag));
      incrementReceivedCounters(json.length());
      BuffersManager.sendMsg(queue, json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    // tagK=tagV based multicasting is not support
  }
}
