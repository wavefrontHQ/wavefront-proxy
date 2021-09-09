package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;


import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api.StructureParser;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 12:55 PM
 */
public class StructureFactory extends AbstractStructureFactory {

  public enum Structure {
    SERVER_CFAPI, SIMPLE, DEFAULT, CLOUDWATCH;

    public static Structure fromString(String text) {
      if (text != null) {
        for (Structure b : Structure.values()) {
          if (text.equalsIgnoreCase(b.name())) {
            return b;
          }
        }
      }
      return SIMPLE;
    }
  }

  public com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api.StructureParser getParser(Structure structure) {
    StructureParser parser = null;
    switch (structure) {
      case SERVER_CFAPI:
        parser = new LogServerApiFormatParser();
        break;
      case DEFAULT:
      case SIMPLE:
        parser = new SimpleJsonFormatParser();
        break;
//      case CLOUDWATCH:
//        parser = new CloudWatchFormatParser();
//        break;
      default:
        break;
    }
    return parser;
  }
}
