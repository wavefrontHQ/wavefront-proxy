package com.wavefront.agent.core.handlers;

import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.formatter.DataFormat;
import com.wavefront.ingester.SpanLogsSerializer;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import wavefront.report.SpanLogs;

/**
 * Handler that processes incoming SpanLogs objects, validates them and hands them over to one of
 * the {@link SenderTask} threads.
 */
public class SpanLogsHandlerImpl extends AbstractReportableEntityHandler<SpanLogs, String> {

  /**
   * Create new instance.
   *
   * @param handlerKey pipeline handler key.
   * @param blockedItemLogger logger for blocked items.
   */
  SpanLogsHandlerImpl(
      final String handler, final QueueInfo handlerKey, @Nullable final Logger blockedItemLogger) {
    super(handler, handlerKey, new SpanLogsSerializer(), blockedItemLogger);
  }

  @Override
  protected void reportInternal(SpanLogs spanLogs) {
    String strSpanLogs = serializer.apply(spanLogs);
    if (strSpanLogs != null) {
      incrementReceivedCounters(strSpanLogs.length());
      BuffersManager.sendMsg(queue, strSpanLogs);
    }
  }
}
