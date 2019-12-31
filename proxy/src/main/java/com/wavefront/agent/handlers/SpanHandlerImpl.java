package com.wavefront.agent.handlers;

import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.ingester.SpanSerializer;
import wavefront.report.Span;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.logging.Logger;

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

  /**
   * @param handlerKey           pipeline hanler key.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written
   *                             into the main log file.
   * @param sendDataTasks        sender tasks.
   * @param validationConfig     parameters for data validation.
   * @param blockedItemLogger    logger for blocked items.
   * @param validItemsLogger     logger for valid items.
   */
  SpanHandlerImpl(final HandlerKey handlerKey,
                  final int blockedItemsPerBatch,
                  final Collection<SenderTask<String>> sendDataTasks,
                  @Nonnull final ValidationConfiguration validationConfig,
                  @Nullable final Logger blockedItemLogger,
                  @Nullable final Logger validItemsLogger) {
    super(handlerKey, blockedItemsPerBatch, new SpanSerializer(), sendDataTasks, true,
        blockedItemLogger);
    this.validationConfig = validationConfig;
    this.validItemsLogger = validItemsLogger;
  }

  @Override
  protected void reportInternal(Span span) {
    validateSpan(span, validationConfig);
    final String strSpan = serializer.apply(span);
    getTask().add(strSpan);
    getReceivedCounter().inc();
    if (validItemsLogger != null) validItemsLogger.info(strSpan);
  }
}
