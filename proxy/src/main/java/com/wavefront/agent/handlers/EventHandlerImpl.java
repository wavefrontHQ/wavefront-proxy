package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.data.Validation;

import java.util.Collection;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import wavefront.report.ReportEvent;

import javax.annotation.Nullable;

/**
 * This class will validate parsed events and distribute them among SenderTask threads.
 *
 * @author vasily@wavefront.com
 */
public class EventHandlerImpl extends AbstractReportableEntityHandler<ReportEvent, ReportEvent> {
  private static final Logger logger = Logger.getLogger(
      AbstractReportableEntityHandler.class.getCanonicalName());
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Function<ReportEvent, String> EVENT_SERIALIZER = value -> {
    try {
      return OBJECT_MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      logger.warning("Serialization error!");
      return null;
    }
  };

  private final Logger validItemsLogger;

  /**
   *
   * @param handlerKey           pipeline key.
   * @param blockedItemsPerBatch number of blocked items that are allowed to be written into the
   *                             main log.
   * @param senderTasks          sender tasks.
   * @param blockedEventsLogger  logger for blocked events.
   * @param validEventsLogger    logger for valid events.
   */
  public EventHandlerImpl(final HandlerKey handlerKey, final int blockedItemsPerBatch,
                          @Nullable final Collection<SenderTask<ReportEvent>> senderTasks,
                          @Nullable final Logger blockedEventsLogger,
                          @Nullable final Logger validEventsLogger) {
    super(handlerKey, blockedItemsPerBatch, EVENT_SERIALIZER, senderTasks, true,
        blockedEventsLogger);
    this.validItemsLogger = validEventsLogger;
  }

  @Override
  protected void reportInternal(ReportEvent event) {
    if (!annotationKeysAreValid(event)) {
      throw new IllegalArgumentException("WF-401: Event annotation key has illegal characters.");
    }
    getTask().add(event);
    getReceivedCounter().inc();
    if (validItemsLogger != null && validItemsLogger.isLoggable(Level.FINEST)) {
      validItemsLogger.info(EVENT_SERIALIZER.apply(event));
    }
  }

  @VisibleForTesting
  static boolean annotationKeysAreValid(ReportEvent event) {
    if (event.getAnnotations() != null) {
      for (String key : event.getAnnotations().keySet()) {
        if (!Validation.charactersAreValid(key)) {
          return false;
        }
      }
    }
    return true;
  }
}
