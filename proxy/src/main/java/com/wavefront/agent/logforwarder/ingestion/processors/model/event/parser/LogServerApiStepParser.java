package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;



import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;

import org.noggit.CharArr;
import org.noggit.JSONParser;

import java.io.IOException;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 1:06 PM
 */
public class LogServerApiStepParser extends BaseApiParser {

  public LogServerApiStepParser(JSONParser parser) {
    super(parser);
  }

  protected void handleCase(JSONParser parser, Event event) throws IOException {
    String key = parser.getString();
    int ev = parser.nextEvent();
    String fieldKey;
    String fieldValue;
    switch (key) {
      // timestamp will be string, it will be converted into a long
      case FieldConstants.TIMESTAMP_FIELD:
        fieldValue = parser.getString();
        if (event.get(key) == null) {
          event.put(key, Long.parseLong(fieldValue));
        }
        break;
      case FieldConstants.STARTING_POS_FIELD:
      case FieldConstants.LENGTH_FIELD:
        fieldKey = key;
        CharArr cArr = parser.getNumberChars();
        char[] data = cArr.getArray();
        fieldValue = new String(data, 0, cArr.size());
        Long value = Long.valueOf(fieldValue);
        event.put(fieldKey, value);
        break;
      case FieldConstants.TEXT_FIELD:
      case FieldConstants.NAME_FIELD:
      case FieldConstants.CONTENT_FIELD:
        if (event.get(key) == null) {
          event.put(key, parseSingleFieldValue(ev));
        }
        break;
      default:
        break;
    }
  }
}