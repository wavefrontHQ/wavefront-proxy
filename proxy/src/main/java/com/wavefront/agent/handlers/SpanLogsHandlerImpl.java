package com.wavefront.agent.handlers;

import com.wavefront.ingester.SpanLogsSerializer;
import wavefront.report.SpanLogs;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Handler that processes incoming SpanLogs objects, validates them and hands them over to one of
 * the {@link SenderTask} threads.
 *
 * @author vasily@wavefront.com
 */
public class SpanLogsHandlerImpl extends AbstractReportableEntityHandler<SpanLogs, String> {
  private final Logger validItemsLogger;

  /**
   * Create new instance.
   *
   * @param handlerKey           pipeline handler key.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written
   *                             into the main log file.
   * @param sendDataTasks        sender tasks.
   * @param receivedRateSink     where to report received rate.
   * @param blockedItemLogger    logger for blocked items.
   * @param validItemsLogger     logger for valid items.
   */
  SpanLogsHandlerImpl(final HandlerKey handlerKey,
                      final int blockedItemsPerBatch,
                      @Nullable final Collection<SenderTask<String>> sendDataTasks,
                      @Nullable final Consumer<Long> receivedRateSink,
                      @Nullable final Logger blockedItemLogger,
                      @Nullable final Logger validItemsLogger) {
    super(handlerKey, blockedItemsPerBatch, new SpanLogsSerializer(),
        sendDataTasks, true, receivedRateSink, blockedItemLogger);
    this.validItemsLogger = validItemsLogger;
  }

  @Override
  protected void reportInternal(SpanLogs spanLogs) {
    String strSpanLogs = serializer.apply(spanLogs);
    if (strSpanLogs != null) {
      getTask().add(strSpanLogs);
      getReceivedCounter().inc();
      if (validItemsLogger != null) validItemsLogger.info(strSpanLogs);
    }
  }
}
