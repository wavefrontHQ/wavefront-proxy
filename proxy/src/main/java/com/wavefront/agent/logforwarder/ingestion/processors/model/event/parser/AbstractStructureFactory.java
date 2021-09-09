package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;


import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api.StructureParser;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 12:56 PM
 */
public abstract class AbstractStructureFactory {

  /**
   *  get structure parser
   *
   * @param structure structure name in string, not null
   * @return corresponding structure parser
   */
  public abstract StructureParser getParser(StructureFactory.Structure structure);

}
