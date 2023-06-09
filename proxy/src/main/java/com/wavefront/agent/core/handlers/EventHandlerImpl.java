package com.wavefront.agent.core.handlers;

import static com.wavefront.agent.PushAgent.isMulticastingActive;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.data.Validation;
import com.wavefront.dto.Event;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wavefront.report.ReportEvent;

/** This class will validate parsed events and distribute them among SenderTask threads. */
public class EventHandlerImpl extends AbstractReportableEntityHandler<ReportEvent, Event> {
  private static final Logger logger =
      LoggerFactory.getLogger(AbstractReportableEntityHandler.class.getCanonicalName());
  private static final Function<ReportEvent, String> EVENT_SERIALIZER =
      value -> new Event(value).toString();

  /**
   * @param handlerKey pipeline key.
   * @param blockedEventsLogger logger for blocked events.
   */
  public EventHandlerImpl(
      final String handler,
      final QueueInfo handlerKey,
      @Nullable final Logger blockedEventsLogger) {
    super(handler, handlerKey, EVENT_SERIALIZER, blockedEventsLogger);
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

    String strEvent = event.toString();
    incrementReceivedCounters(strEvent.length());
    BuffersManager.sendMsg(queue, strEvent);

    if (isMulticastingActive
        && event.getAnnotations() != null
        && event.getAnnotations().containsKey(MULTICASTING_TENANT_TAG_KEY)) {
      String[] multicastingTenantNames =
          event.getAnnotations().get(MULTICASTING_TENANT_TAG_KEY).trim().split(",");
      event.getAnnotations().remove(MULTICASTING_TENANT_TAG_KEY);
      for (String tenant : multicastingTenantNames) {
        QueueInfo tenantQueue = queue.getTenantQueue(tenant);
        if (tenantQueue != null) {
          BuffersManager.sendMsg(tenantQueue, strEvent);
        } else {
          logger.info("Tenant '" + tenant + "' invalid");
        }
      }
    }
  }
}
