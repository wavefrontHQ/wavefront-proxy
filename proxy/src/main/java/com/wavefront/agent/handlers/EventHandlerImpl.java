package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.data.Validation;
import com.wavefront.ingester.ReportSourceTagSerializer;

import java.util.Collection;
import java.util.function.Function;
import java.util.logging.Logger;

import wavefront.report.Event;
import wavefront.report.ReportSourceTag;

/**
 * This class will validate parsed events and distribute them among SenderTask threads.
 *
 * @author vasily@wavefront.com
 */
public class EventHandlerImpl extends AbstractReportableEntityHandler<Event> {
  private static final Logger logger = Logger.getLogger(
      AbstractReportableEntityHandler.class.getCanonicalName());

  private static final ObjectMapper JSON_PARSER = new ObjectMapper();
  private static final Function<Event, String> EVENT_SERIALIZER = value -> {
    try {
      return JSON_PARSER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      logger.warning("Serialization error!");
      return null;
    }
  };

  public EventHandlerImpl(final String handle, final int blockedItemsPerBatch,
                                    final Collection<SenderTask> senderTasks) {
    super(ReportableEntityType.SOURCE_TAG, handle, blockedItemsPerBatch,
        EVENT_SERIALIZER, senderTasks, null, null, true);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void reportInternal(Event event) {
    if (!annotationKeysAreValid(event)) {
      throw new IllegalArgumentException("WF-401: Event annotation key has illegal characters.");
    }
    getTask().add(event);
  }

  @VisibleForTesting
  static boolean annotationKeysAreValid(Event event) {
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
