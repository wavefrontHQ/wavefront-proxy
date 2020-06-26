package com.wavefront.agent.handlers;

import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.ingester.SpanSerializer;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.Consumer;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.Span;

import static com.wavefront.data.Validation.validateSpan;

/**
 * Handler that processes incoming Span objects, validates them and hands them over to one of
 * the {@link SenderTask} threads.
 *
 * @author vasily@wavefront.com
 */
public class SpanHandlerImpl extends AbstractReportableEntityHandler<Span, String> {

  private final ValidationConfiguration validationConfig;
  private final Logger validItemsLogger;
  private final Supplier<Integer> dropSpansDelayedMinutes;

  /**
   * @param handlerKey           pipeline hanler key.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written
   *                             into the main log file.
   * @param sendDataTasks        sender tasks.
   * @param validationConfig     parameters for data validation.
   * @param receivedRateSink     where to report received rate.
   * @param blockedItemLogger    logger for blocked items.
   * @param validItemsLogger     logger for valid items.
   */
  SpanHandlerImpl(final HandlerKey handlerKey,
                  final int blockedItemsPerBatch,
                  final Collection<SenderTask<String>> sendDataTasks,
                  @Nonnull final ValidationConfiguration validationConfig,
                  @Nullable final Consumer<Long> receivedRateSink,
                  @Nullable final Logger blockedItemLogger,
                  @Nullable final Logger validItemsLogger,
                  @Nonnull final Supplier<Integer> dropSpansDelayedMinutes) {
    super(handlerKey, blockedItemsPerBatch, new SpanSerializer(), sendDataTasks, true,
        receivedRateSink, blockedItemLogger);
    this.validationConfig = validationConfig;
    this.validItemsLogger = validItemsLogger;
    this.dropSpansDelayedMinutes = dropSpansDelayedMinutes;
  }

  @Override
  protected void reportInternal(Span span) {
    Integer maxSpanDelay = dropSpansDelayedMinutes.get();
    if (maxSpanDelay != null && span.getStartMillis() + span.getDuration() <
        Clock.now() - TimeUnit.MINUTES.toMillis(maxSpanDelay)) {
      this.reject(span, "span is older than acceptable delay of " + maxSpanDelay + " minutes");
      return;
    }
    validateSpan(span, validationConfig);
    final String strSpan = serializer.apply(span);
    getTask().add(strSpan);
    getReceivedCounter().inc();
    if (validItemsLogger != null) validItemsLogger.info(strSpan);
  }
}
