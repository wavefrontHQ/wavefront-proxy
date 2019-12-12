package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.data.Validation;

import java.util.Collection;
import java.util.function.Function;
import java.util.logging.Logger;

import wavefront.report.ReportEvent;

/**
 * This class will validate parsed events and distribute them among SenderTask threads.
 *
 * @author vasily@wavefront.com
 */
public class EventHandlerImpl extends AbstractReportableEntityHandler<ReportEvent, ReportEvent> {
  private static final Logger logger = Logger.getLogger(
      AbstractReportableEntityHandler.class.getCanonicalName());

  private static final ObjectMapper JSON_PARSER = new ObjectMapper();
  private static final Function<ReportEvent, String> EVENT_SERIALIZER = value -> {
    try {
      return JSON_PARSER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      logger.warning("Serialization error!");
      return null;
    }
  };

  public EventHandlerImpl(final String handle, final int blockedItemsPerBatch,
                          final Collection<SenderTask<ReportEvent>> senderTasks,
                          final Logger blockedEventsLogger) {
    super(ReportableEntityType.EVENT, handle, blockedItemsPerBatch,
        EVENT_SERIALIZER, senderTasks, null, null, true, blockedEventsLogger);
  }

  @Override
  protected void reportInternal(ReportEvent event) {
    if (!annotationKeysAreValid(event)) {
      throw new IllegalArgumentException("WF-401: Event annotation key has illegal characters.");
    }
    getTask().add(event);
    getReceivedCounter().inc();
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
