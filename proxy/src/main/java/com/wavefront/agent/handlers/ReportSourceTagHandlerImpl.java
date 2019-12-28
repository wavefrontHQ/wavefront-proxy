package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.data.ReportableEntityType;
import com.wavefront.data.Validation;
import com.wavefront.ingester.ReportSourceTagSerializer;

import java.util.Collection;
import java.util.logging.Logger;

import wavefront.report.ReportSourceTag;

/**
 * This class will validate parsed source tags and distribute them among SenderTask threads.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 * @author vasily@wavefront.com
 */
class ReportSourceTagHandlerImpl
    extends AbstractReportableEntityHandler<ReportSourceTag, ReportSourceTag> {

  private static final Logger logger = Logger.getLogger(
      AbstractReportableEntityHandler.class.getCanonicalName());

  public ReportSourceTagHandlerImpl(final String handle, final int blockedItemsPerBatch,
                                    final Collection<SenderTask<ReportSourceTag>> senderTasks,
                                    final Logger blockedItemLogger) {
    super(ReportableEntityType.SOURCE_TAG, handle, blockedItemsPerBatch,
        new ReportSourceTagSerializer(), senderTasks, null, null,
        true, blockedItemLogger);
  }

  @Override
  protected void reportInternal(ReportSourceTag sourceTag) {
    if (!annotationKeysAreValid(sourceTag)) {
      throw new IllegalArgumentException("WF-401: SourceTag annotation key has illegal characters.");
    }
    getTask(sourceTag).add(sourceTag);
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

  private SenderTask<ReportSourceTag> getTask(ReportSourceTag sourceTag) {
    // we need to make sure the we preserve the order of operations for each source
    return senderTasks.get(Math.abs(sourceTag.getSource().hashCode()) % senderTasks.size());
  }
}
