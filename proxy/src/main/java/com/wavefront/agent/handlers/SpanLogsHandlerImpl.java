package com.wavefront.agent.handlers;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * Handler that processes incoming SpanLogs objects, validates them and hands them over to one of
 * the {@link SenderTask} threads.
 *
 * @author vasily@wavefront.com
 */
public class SpanLogsHandlerImpl extends AbstractReportableEntityHandler<SpanLogs, String> {
  private static final Logger logger = Logger.getLogger(
      AbstractReportableEntityHandler.class.getCanonicalName());
  private static final ObjectMapper JSON_PARSER = new ObjectMapper();

  static {
    JSON_PARSER.addMixIn(SpanLogs.class, IgnoreSchemaProperty.class);
    JSON_PARSER.addMixIn(SpanLog.class, IgnoreSchemaProperty.class);
  }

  private static final Function<SpanLogs, String> SPAN_LOGS_SERIALIZER = value -> {
    try {
      return JSON_PARSER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      logger.warning("Serialization error!");
      return null;
    }
  };

  private final Logger validItemsLogger;

  /**
   * Create new instance.
   *
   * @param handlerKey           pipeline handler key.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written
   *                             into the main log file.
   * @param sendDataTasks        sender tasks.
   * @param blockedItemLogger    logger for blocked items.
   * @param validItemsLogger     logger for valid items.
   */
  SpanLogsHandlerImpl(final HandlerKey handlerKey,
                      final int blockedItemsPerBatch,
                      @Nullable final Collection<SenderTask<String>> sendDataTasks,
                      @Nullable final Logger blockedItemLogger,
                      @Nullable final Logger validItemsLogger) {
    super(handlerKey, blockedItemsPerBatch, SPAN_LOGS_SERIALIZER,
        sendDataTasks, true, blockedItemLogger);
    this.validItemsLogger = validItemsLogger;
  }

  @Override
  protected void reportInternal(SpanLogs spanLogs) {
    String strSpanLogs = SPAN_LOGS_SERIALIZER.apply(spanLogs);
    getTask().add(strSpanLogs);
    getReceivedCounter().inc();
    if (validItemsLogger != null) validItemsLogger.info(strSpanLogs);
  }

  abstract static class IgnoreSchemaProperty {
    @JsonIgnore
    abstract void getSchema();
  }
}
