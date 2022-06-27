package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.buffer.BuffersManager;
import com.wavefront.data.Validation;
import com.wavefront.dto.SourceTag;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.logging.Logger;
import javax.annotation.Nullable;
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
      HandlerKey handlerKey,
      final int blockedItemsPerBatch,
      @Nullable final BiConsumer<String, Long> receivedRateSink,
      final Logger blockedItemLogger) {
    super(
        handlerKey,
        blockedItemsPerBatch,
        SOURCE_TAG_SERIALIZER,
        true,
        receivedRateSink,
        blockedItemLogger);
  }

  @Override
  protected void reportInternal(ReportSourceTag sourceTag) {
    if (!annotationsAreValid(sourceTag)) {
      throw new IllegalArgumentException(
          "WF-401: SourceTag annotation key has illegal characters.");
    }

    getReceivedCounter().inc();
    BuffersManager.sendMsg(handlerKey, sourceTag.toString());

    getReceivedCounter().inc();
    // tagK=tagV based multicasting is not support
  }

  @VisibleForTesting
  static boolean annotationsAreValid(ReportSourceTag sourceTag) {
    if (sourceTag.getOperation() == SourceOperationType.SOURCE_DESCRIPTION) return true;
    return sourceTag.getAnnotations().stream().allMatch(Validation::charactersAreValid);
  }
}
