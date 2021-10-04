package com.wavefront.agent.logforwarder.ingestion.processors;

import com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventPayload;

import org.json.simple.JSONAware;

/**
 * A test processor to simulate {@link Processor} process callback with event payload.
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 10/4/21 10:07 AM
 */
public class LogIngestionTestProcessor implements Processor {

  public static EventPayload eventPayload;

  @Override
  public void initializeProcessor(JSONAware json) throws Throwable {

  }

  @Override
  public EventPayload process(EventPayload eventPayload) throws Exception {
    LogIngestionTestProcessor.eventPayload = eventPayload;
    return eventPayload;
  }
}
