package com.wavefront.agent.core.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.data.Validation;
import com.wavefront.dto.SourceTag;
import java.util.function.Function;
import java.util.logging.Logger;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;

/**
 * This class will validate parsed source tags and distribute them among SenderTask threads.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 * @author vasily@wavefront.com
 */
class ReportSourceTagHandlerImpl
    extends AbstractReportableEntityHandler<ReportSourceTag, SourceTag> {
  private static final Function<ReportSourceTag, String> SOURCE_TAG_SERIALIZER =
      value -> new SourceTag(value).toString();

  public ReportSourceTagHandlerImpl(
      String handler,
      QueueInfo handlerKey,
      final int blockedItemsPerBatch,
      final Logger blockedItemLogger) {
    super(
        handler, handlerKey, blockedItemsPerBatch, SOURCE_TAG_SERIALIZER, true, blockedItemLogger);
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

    getReceivedCounter().inc();
    BuffersManager.sendMsg(queue, sourceTag.toString());

    getReceivedCounter().inc();
    // tagK=tagV based multicasting is not support
  }
}
