package com.wavefront.agent.logforwarder.ingestion.processors;



import com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventPayload;

import org.json.simple.JSONAware;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/1/21 5:52 PM
 */
public interface Processor {

  public EventPayload process(EventPayload eventPayload) throws Throwable;

  public void initializeProcessor(JSONAware jsonObject) throws Throwable;
}
