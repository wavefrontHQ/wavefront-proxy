package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.data.Validation;
import com.wavefront.dto.SourceTag;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * This class will validate parsed source tags and distribute them among SenderTask threads.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 * @author vasily@wavefront.com
 */
class ReportSourceTagHandlerImpl
    extends AbstractReportableEntityHandler<ReportSourceTag, SourceTag> {
  private static final Function<ReportSourceTag, String> SOURCE_TAG_SERIALIZER = value ->
      new SourceTag(value).toString();

  public ReportSourceTagHandlerImpl(HandlerKey handlerKey, final int blockedItemsPerBatch,
                                    @Nullable final Collection<SenderTask<SourceTag>> senderTasks,
                                    @Nullable final Consumer<Long> receivedRateSink,
                                    final Logger blockedItemLogger) {
    super(handlerKey, blockedItemsPerBatch, SOURCE_TAG_SERIALIZER, senderTasks, true,
        receivedRateSink, blockedItemLogger);
  }

  @Override
  protected void reportInternal(ReportSourceTag sourceTag) {
    if (!annotationsAreValid(sourceTag)) {
      throw new IllegalArgumentException("WF-401: SourceTag annotation key has illegal characters.");
    }
    getTask(sourceTag).add(new SourceTag(sourceTag));
    getReceivedCounter().inc();
  }

  @VisibleForTesting
  static boolean annotationsAreValid(ReportSourceTag sourceTag) {
    if (sourceTag.getOperation() == SourceOperationType.SOURCE_DESCRIPTION) return true;
    return sourceTag.getAnnotations().stream().allMatch(Validation::charactersAreValid);
  }

  private SenderTask<SourceTag> getTask(ReportSourceTag sourceTag) {
    // we need to make sure the we preserve the order of operations for each source
    return senderTasks.get(Math.abs(sourceTag.getSource().hashCode()) % senderTasks.size());
  }
}
