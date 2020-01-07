package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.data.Validation;
import com.wavefront.dto.SourceTag;
import com.wavefront.ingester.ReportSourceTagSerializer;

import java.util.Collection;
import java.util.logging.Logger;

import wavefront.report.ReportSourceTag;

import javax.annotation.Nullable;

/**
 * This class will validate parsed source tags and distribute them among SenderTask threads.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 * @author vasily@wavefront.com
 */
class ReportSourceTagHandlerImpl
    extends AbstractReportableEntityHandler<ReportSourceTag, SourceTag> {

  public ReportSourceTagHandlerImpl(
      HandlerKey handlerKey, final int blockedItemsPerBatch,
      @Nullable final Collection<SenderTask<SourceTag>> senderTasks,
      final Logger blockedItemLogger) {
    super(handlerKey, blockedItemsPerBatch, new ReportSourceTagSerializer(), senderTasks, true,
        blockedItemLogger);
  }

  @Override
  protected void reportInternal(ReportSourceTag sourceTag) {
    if (!annotationKeysAreValid(sourceTag)) {
      throw new IllegalArgumentException("WF-401: SourceTag annotation key has illegal characters.");
    }
    getTask(sourceTag).add(new SourceTag(sourceTag));
  }

  @VisibleForTesting
  static boolean annotationKeysAreValid(ReportSourceTag sourceTag) {
    if (sourceTag.getAnnotations() != null) {
      for (String key : sourceTag.getAnnotations()) {
        if (!Validation.charactersAreValid(key)) {
          return false;
        }
      }
    }
    return true;
  }

  private SenderTask<SourceTag> getTask(ReportSourceTag sourceTag) {
    // we need to make sure the we preserve the order of operations for each source
    return senderTasks.get(Math.abs(sourceTag.getSource().hashCode()) % senderTasks.size());
  }
}
