package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api;


import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 12:53 PM
 */
public interface EventIterable extends Iterable<Event> {
  Event getMetaEvent();
}

