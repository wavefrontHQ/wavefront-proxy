package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api;


import java.io.IOException;
import java.io.Reader;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 12:54 PM
 */
public interface StructureParser {

  EventIterable parse(Reader reader) throws IOException;
}
