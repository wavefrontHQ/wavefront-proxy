package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.data.ReportableEntityType;
import com.wavefront.data.Validation;
import com.wavefront.ingester.ReportSourceTagSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import wavefront.report.ReportSourceTag;

/**
 * This class will validate parsed source tags and distribute them among SenderTask threads.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 * @author vasily@wavefront.com
 */
public class ReportSourceTagHandlerImpl extends AbstractReportableEntityHandler<ReportSourceTag> {

  private static final Logger logger = Logger.getLogger(AbstractReportableEntityHandler.class.getCanonicalName());

  private final Counter attemptedCounter;
  private final Counter queuedCounter;

  public ReportSourceTagHandlerImpl(final String handle, final int blockedItemsPerBatch,
                                    final Collection<SenderTask> senderTasks) {
    super(ReportableEntityType.SOURCE_TAG, handle, blockedItemsPerBatch, new ReportSourceTagSerializer(), senderTasks);
    this.attemptedCounter = Metrics.newCounter(new MetricName("sourceTags." + handle, "", "sent"));
    this.queuedCounter = Metrics.newCounter(new MetricName("sourceTags." + handle, "", "queued"));

    statisticOutputExecutor.scheduleAtFixedRate(this::printStats, 10, 10, TimeUnit.SECONDS);
    statisticOutputExecutor.scheduleAtFixedRate(this::printTotal, 1, 1, TimeUnit.MINUTES);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void reportInternal(ReportSourceTag sourceTag) {
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

  private void printStats() {
    logger.info("[" + this.handle + "] sourceTags received rate: " + getReceivedOneMinuteRate() +
        " pps (1 min), " + getReceivedFiveMinuteRate() + " pps (5 min), " +
        this.receivedBurstRateCurrent + " pps (current).");
  }

  private void printTotal() {
    logger.info("[" + this.handle + "] Total sourceTags processed since start: " + this.attemptedCounter.count() +
        "; blocked: " + this.blockedCounter.count());
  }

  private SenderTask getTask(ReportSourceTag sourceTag) {
    // we need to make sure the we preserve the order of operations for each source
    return senderTasks.get(Math.abs(sourceTag.getSource().hashCode()) % senderTasks.size());
  }
}
