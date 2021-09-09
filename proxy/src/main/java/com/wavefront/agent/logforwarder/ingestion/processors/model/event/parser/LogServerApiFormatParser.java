package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;



import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api.EventIterable;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api.LogEventIterable;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api.StructureParser;

import org.noggit.JSONParser;
import java.io.IOException;
import java.io.Reader;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 1:05 PM
 */
public class LogServerApiFormatParser implements StructureParser {

  @Override
  public EventIterable parse(Reader reader) throws IOException {
    JSONParser parser = new JSONParser(reader);
    return new LogEventIterable(new LogServerApiStepParser(parser));
  }

}
