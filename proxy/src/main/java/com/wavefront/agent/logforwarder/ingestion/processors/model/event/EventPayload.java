package com.wavefront.agent.logforwarder.ingestion.processors.model.event;

import com.google.common.collect.Lists;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 8/30/21 7:33 PM
 */
public class EventPayload {

  public EventBatch batch;

  public String tenantId;

  public String orgId;

  public long payloadSizeInBytes;

  public int retryCount = 0;

  public Map<String, String> requestHeaders;

  public PayloadType payloadType;
  //true if the event-payload is billable, false if not ,default is true
  public Boolean billable = true;
  //Optional. Used for reading payload body(once)
  public transient Reader inputReader;
  // Used for tracking the additional bytes that are added by Event Tagger which were not available at ingestion
  public transient long additionalTaggedBytes;

  private Event metaEvent = null;

  public Map<String /*ID*/, String /*lower case text*/> textLowerCaseMap = new HashMap<>();

  public EventPayload(String tenantId,
                      String orgId,
                      PayloadType payloadType,
                      EventBatch batch,
                      long payloadSizeInBytes,
                      Map<String, String> requestHeaders,
                      Boolean billable) {
    this.tenantId = tenantId;
    this.orgId = orgId;
    this.batch = batch;
    this.payloadType = payloadType;
    this.payloadSizeInBytes = payloadSizeInBytes;
    this.requestHeaders = requestHeaders;
    this.billable = billable;
  }

  public EventPayload(EventBatch batch) {
    this("unknown", "unknown", PayloadType.LOGS, batch, -1, new HashMap<>(), false);
  }

  public static EventPayload createEventPayload(String body,
                                                String tenantId,
                                                String orgId,
                                                PayloadType payloadType,
                                                long payloadSizeInBytes,
                                                Map<String, String> requestHeaders,
                                                Boolean billable) {
    Event e = new Event();
    e.put("body", body);
    EventBatch batch = new EventBatch();
    batch.add(e);
    return new EventPayload(tenantId, orgId, payloadType, batch, payloadSizeInBytes, requestHeaders, billable);
  }

  /**
   * Method to split given Event Payload into batches of sub Payloads
   * based on the number of events per sub payload.
   * <p>
   * For ex: A given payload with 500 events be bifurcated into 10 sub payloads
   * if the split size is 10
   *
   * @param splitSize
   * @return
   */
  public List<EventPayload> split(int splitSize) {
    List<List<Event>> batchedEvents
        = Lists.partition(this.batch, splitSize);

    List<EventPayload> batchedPayloads = new ArrayList<>();

    batchedEvents
        .forEach(batch -> {
          EventPayload batchEventPayload = new EventPayload(new EventBatch());
          batchEventPayload.batch.addAll(batch);
          batchEventPayload.tenantId = this.tenantId;
          batchEventPayload.orgId = this.orgId;
          batchEventPayload.payloadSizeInBytes = this.payloadSizeInBytes;
          batchEventPayload.retryCount = this.retryCount;
          batchEventPayload.requestHeaders = this.requestHeaders;
          batchEventPayload.payloadType = this.payloadType;
          batchEventPayload.metaEvent = this.metaEvent;
          batchEventPayload.billable = this.billable;
          batchEventPayload.additionalTaggedBytes = this.additionalTaggedBytes;
          batchedPayloads.add(batchEventPayload);
        });

    return batchedPayloads;
  }

  public boolean isRetried() {
    return this.retryCount > 0;
  }

  @Override
  public String toString() {
    return "EventPayload [batch=" + batch + ", tenantId=" + tenantId
        + ", payloadSizeInBytes=" + payloadSizeInBytes
        + ", orgId=" + orgId + ", retryCount=" + retryCount
        + ", requestHeaders=" + requestHeaders
        + ", metaEvent=" + metaEvent + "]";
  }

  /**
   * Clones this object. Does not do a recursive deep clone of the
   * {@link #EventPayload(String, String, EventPayload.PayloadType, com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventBatch, long, Map, Boolean)}}
   */
  @Override
  public EventPayload clone() {
    EventBatch eventBatch = this.batch;
    EventBatch clonedEventBatch = new EventBatch();

    eventBatch.forEach(event -> {
      clonedEventBatch.add(event.clone());
    });

    return clone(clonedEventBatch);
  }

  /**
   * Clones this object without batch.
   */
  public EventPayload cloneWithoutBatch() {
    return clone(new EventBatch());
  }

  /**
   * Clones this object with the input eventBatch.
   */
  private EventPayload clone(EventBatch eventBatch) {
    Map<String, String> clonedRequestHeaders = new HashMap<>();
    clonedRequestHeaders.putAll(this.requestHeaders);

    EventPayload clonedEventPayload = new EventPayload(
        this.tenantId,
        this.orgId,
        this.payloadType,
        eventBatch,
        this.payloadSizeInBytes,
        clonedRequestHeaders,
        this.billable);

    if (this.metaEvent != null) {
      Event clonedMetaEvent = new Event();
      clonedMetaEvent.putAll(this.metaEvent);
      clonedEventPayload.setMetaEvent(clonedMetaEvent);
    }

    return clonedEventPayload;
  }

  /**
   * If true then there is a need to re-index the payload
   */
  public boolean isIndexDirty() {
    return this.batch != null && this.batch.isDirty();
  }

  public Event getMetaEvent() {
    return this.metaEvent;
  }

  public void setMetaEvent(Event metaEvent) {
    this.metaEvent = metaEvent;
  }

  public static enum PayloadType {
    LOGS,
    EVENTS,
    GRAPH
  }
}