package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api;


import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.StepParser;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.iterator.LogEventIterator;

import java.util.Iterator;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 1:02 PM
 */
public class LogEventIterable implements EventIterable {

  private StepParser stepParser;

  public LogEventIterable(StepParser stepParser) {
    this.stepParser = stepParser;
  }

  public Iterator<Event> iterator() {
    return new LogEventIterator(this.stepParser);
  }

  @Override
  public Event getMetaEvent() {
    return null;
  }
}
