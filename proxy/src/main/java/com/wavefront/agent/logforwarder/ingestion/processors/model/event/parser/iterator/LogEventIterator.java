package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.iterator;


import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.StepParser;

import java.util.Iterator;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 1:03 PM
 */
public class LogEventIterator implements Iterator<Event> {
  protected Event nextItem;
  protected StepParser parser;

  public LogEventIterator(StepParser parser) {
    this.parser = parser;
    this.nextItem = parser.parseNext();
  }

  @Override
  public boolean hasNext() {
    return this.nextItem != null;
  }

  @Override
  public Event next() {
    if (this.nextItem == null) {
      return null;
    }

    Event result = this.nextItem;
    this.nextItem = parser.parseNext();
    return result;
  }
}
