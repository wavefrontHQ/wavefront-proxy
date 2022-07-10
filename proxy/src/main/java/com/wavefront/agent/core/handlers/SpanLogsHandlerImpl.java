package com.wavefront.agent.core.handlers;

import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.senders.SenderTask;
import com.wavefront.ingester.SpanLogsSerializer;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import wavefront.report.SpanLogs;

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
   * @param handlerKey pipeline handler key.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into
   *     the main log file.
   * @param blockedItemLogger logger for blocked items.
   * @param validItemsLogger logger for valid items.
   */
  SpanLogsHandlerImpl(
      final String handler,
      final QueueInfo handlerKey,
      final int blockedItemsPerBatch,
      @Nullable final Logger blockedItemLogger,
      @Nullable final Logger validItemsLogger) {
    super(
        handler,
        handlerKey,
        blockedItemsPerBatch,
        new SpanLogsSerializer(),
        true,
        blockedItemLogger);
    this.validItemsLogger = validItemsLogger;
  }

  @Override
  protected void reportInternal(SpanLogs spanLogs) {
    String strSpanLogs = serializer.apply(spanLogs);
    if (strSpanLogs != null) {

      getReceivedCounter().inc();
      BuffersManager.sendMsg(queue, strSpanLogs);

      getReceivedCounter().inc();
      if (validItemsLogger != null) validItemsLogger.info(strSpanLogs);
      // tagK=tagV based multicasting is not supported
    }
  }
}
