package com.wavefront.agent.handlers;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.ingester.SpanLogsSerializer;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
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
   * @param senderTaskMap map of tenant name and tasks actually handling data transfer to the
   *     Wavefront endpoint corresponding to the tenant name
   * @param receivedRateSink where to report received rate.
   * @param blockedItemLogger logger for blocked items.
   * @param validItemsLogger logger for valid items.
   */
  SpanLogsHandlerImpl(
      final HandlerKey handlerKey,
      final int blockedItemsPerBatch,
      @Nullable final Map<String, Collection<SenderTask<String>>> senderTaskMap,
      @Nullable final BiConsumer<String, Long> receivedRateSink,
      @Nullable final Logger blockedItemLogger,
      @Nullable final Logger validItemsLogger) {
    super(
        handlerKey,
        blockedItemsPerBatch,
        new SpanLogsSerializer(),
        senderTaskMap,
        true,
        receivedRateSink,
        blockedItemLogger);
    this.validItemsLogger = validItemsLogger;
  }

  @Override
  protected void reportInternal(SpanLogs spanLogs) {
    String strSpanLogs = serializer.apply(spanLogs);
    if (strSpanLogs != null) {
      getTask(APIContainer.CENTRAL_TENANT_NAME).add(strSpanLogs);
      getReceivedCounter().inc();
      if (validItemsLogger != null) validItemsLogger.info(strSpanLogs);
      // tagK=tagV based multicasting is not supported
    }
  }
}
