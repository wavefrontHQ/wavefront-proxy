package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.data.Validation;
import com.wavefront.dto.Event;
import wavefront.report.ReportEvent;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class will validate parsed events and distribute them among SenderTask threads.
 *
 * @author vasily@wavefront.com
 */
public class EventHandlerImpl extends AbstractReportableEntityHandler<ReportEvent, Event> {
  private static final Logger logger = Logger.getLogger(
      AbstractReportableEntityHandler.class.getCanonicalName());
  private static final Function<ReportEvent, String> EVENT_SERIALIZER = value ->
      new Event(value).toString();

  private final Logger validItemsLogger;

  /**
   * @param handlerKey           pipeline key.
   * @param blockedItemsPerBatch number of blocked items that are allowed to be written into the
   *                             main log.
   * @param senderTasks          sender tasks.
   * @param receivedRateSink     where to report received rate.
   * @param blockedEventsLogger  logger for blocked events.
   * @param validEventsLogger    logger for valid events.
   */
  public EventHandlerImpl(final HandlerKey handlerKey, final int blockedItemsPerBatch,
                          @Nullable final Collection<SenderTask<Event>> senderTasks,
                          @Nullable final Consumer<Long> receivedRateSink,
                          @Nullable final Logger blockedEventsLogger,
                          @Nullable final Logger validEventsLogger) {
    super(handlerKey, blockedItemsPerBatch, EVENT_SERIALIZER, senderTasks, true, receivedRateSink,
        blockedEventsLogger);
    this.validItemsLogger = validEventsLogger;
  }

  @Override
  protected void reportInternal(ReportEvent event) {
    if (!annotationKeysAreValid(event)) {
      throw new IllegalArgumentException("WF-401: Event annotation key has illegal characters.");
    }
    getTask().add(new Event(event));
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
