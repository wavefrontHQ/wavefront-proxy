package com.wavefront.agent.core.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.data.Validation;
import com.wavefront.dto.Event;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import wavefront.report.ReportEvent;

/** This class will validate parsed events and distribute them among SenderTask threads. */
public class EventHandlerImpl extends AbstractReportableEntityHandler<ReportEvent, Event> {
  private static final Logger logger =
      Logger.getLogger(AbstractReportableEntityHandler.class.getCanonicalName());
  private static final Function<ReportEvent, String> EVENT_SERIALIZER =
      value -> new Event(value).toString();

  private final Logger validItemsLogger;

  /**
   * @param handlerKey pipeline key.
   * @param blockedItemsPerBatch number of blocked items that are allowed to be written into the
   *     main log.
   * @param blockedEventsLogger logger for blocked events.
   * @param validEventsLogger logger for valid events.
   */
  public EventHandlerImpl(
      final int port,
      final QueueInfo handlerKey,
      final int blockedItemsPerBatch,
      @Nullable final Logger blockedEventsLogger,
      @Nullable final Logger validEventsLogger) {
    super(port, handlerKey, blockedItemsPerBatch, EVENT_SERIALIZER, true, blockedEventsLogger);
    this.validItemsLogger = validEventsLogger;
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

  @Override
  protected void reportInternal(ReportEvent event) {
    if (!annotationKeysAreValid(event)) {
      throw new IllegalArgumentException("WF-401: Event annotation key has illegal characters.");
    }

    getReceivedCounter().inc();
    BuffersManager.sendMsg(handlerKey, event.toString());

    if (validItemsLogger != null && validItemsLogger.isLoggable(Level.FINEST)) {
      validItemsLogger.info(EVENT_SERIALIZER.apply(event));
    }
  }
}
