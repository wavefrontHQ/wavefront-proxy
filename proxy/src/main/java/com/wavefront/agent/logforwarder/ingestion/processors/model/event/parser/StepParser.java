package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;


import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 1:00 PM
 */
public interface StepParser {
  public static String LI_MESSAGE_FIELD_ID = "id_log_intelligence_message";

  public static String transformEventFieldKey(String key) {
    return "id".equals(key) ? LI_MESSAGE_FIELD_ID : key;
  }

  Event parseNext();
}
