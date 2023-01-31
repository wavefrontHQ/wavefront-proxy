package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.data.Validation;
import com.wavefront.dto.Event;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import wavefront.report.ReportEvent;

/**
 * This class will validate parsed events and distribute them among SenderTask threads.
 *
 * @author vasily@wavefront.com
 */
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
   * @param senderTaskMap map of tenant name and tasks actually handling data transfer to the
   *     Wavefront endpoint corresponding to the tenant name
   * @param receivedRateSink where to report received rate.
   * @param blockedEventsLogger logger for blocked events.
   * @param validEventsLogger logger for valid events.
   */
  public EventHandlerImpl(
      final HandlerKey handlerKey,
      final int blockedItemsPerBatch,
      @Nullable final Map<String, Collection<SenderTask<Event>>> senderTaskMap,
      @Nullable final BiConsumer<String, Long> receivedRateSink,
      @Nullable final Logger blockedEventsLogger,
      @Nullable final Logger validEventsLogger) {
    super(
        handlerKey,
        blockedItemsPerBatch,
        EVENT_SERIALIZER,
        senderTaskMap,
        true,
        receivedRateSink,
        blockedEventsLogger);
    super.initializeCounters();
    this.validItemsLogger = validEventsLogger;
  }

  @Override
  protected void reportInternal(ReportEvent event) {
    if (!annotationKeysAreValid(event)) {
      throw new IllegalArgumentException("WF-401: Event annotation key has illegal characters.");
    }
    Event eventToAdd = new Event(event);
    getTask(APIContainer.CENTRAL_TENANT_NAME).add(eventToAdd);
    getReceivedCounter().inc();
    // check if event annotations contains the tag key indicating this event should be
    // multicasted
    if (isMulticastingActive
        && event.getAnnotations() != null
        && event.getAnnotations().containsKey(MULTICASTING_TENANT_TAG_KEY)) {
      String[] multicastingTenantNames =
          event.getAnnotations().get(MULTICASTING_TENANT_TAG_KEY).trim().split(",");
      event.getAnnotations().remove(MULTICASTING_TENANT_TAG_KEY);
      for (String multicastingTenantName : multicastingTenantNames) {
        // if the tenant name indicated in event tag is not configured, just ignore
        if (getTask(multicastingTenantName) != null) {
          getTask(multicastingTenantName).add(new Event(event));
        }
      }
    }
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
