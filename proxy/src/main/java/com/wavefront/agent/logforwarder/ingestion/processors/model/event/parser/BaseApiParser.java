package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;



import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;

import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 1:09 PM
 */
public class BaseApiParser implements StepParser {
  private static final String POSITIONAL_FIELD_SUFFIX = "_xxxx";
  private JSONParser parser;
  private Stack<Integer> stack = new Stack<>();

  public BaseApiParser(JSONParser parser) {
    this.parser = parser;
  }

  protected static void extractPositionalInfo(Event fieldMap) {
    String fieldKey = (String) fieldMap.get(FieldConstants.NAME_FIELD);
    Object content = fieldMap.get(FieldConstants.CONTENT_FIELD);
    if (content != null) {
      // remove all entries
      fieldMap.clear();

      // add content entry
      fieldMap.put(fieldKey, content);
    } else {
      int startPos = Math.toIntExact((long) fieldMap.get(FieldConstants.STARTING_POS_FIELD));
      int length = Math.toIntExact((long) fieldMap.get(FieldConstants.LENGTH_FIELD));

      int[] valuesArray = new int[2];
      valuesArray[0] = startPos;
      valuesArray[1] = startPos + length;

      ArrayList<Integer> values = new ArrayList<Integer>() {
        private static final long serialVersionUID = 1L;

        {
          for (int i : valuesArray) {
            add(i);
          }
        }
      };

      // remove all entries
      fieldMap.clear();

      // add positional entry
      fieldMap.put(fieldKey + POSITIONAL_FIELD_SUFFIX, values);
    }
  }

  protected static Event addPositionalInfo(Event event) {

    String text = (String) event.get(FieldConstants.TEXT_FIELD);

    Event newFields = new Event();

    // remove temp fields
    for (Iterator<String> iterator = event.keySet().iterator(); iterator.hasNext(); ) {

      String fieldName = iterator.next();

      if (fieldName.endsWith(POSITIONAL_FIELD_SUFFIX)) {

        Object o = event.get(fieldName);

        if (o instanceof List) {
          @SuppressWarnings("unchecked")
          List<Object> values = (List<Object>) o;

          String newFieldName = fieldName.substring(0, fieldName.length() - 5);
          int startPos = (int) values.get(0);
          int endPos = (int) values.get(1);

          // lengths were valid
          if (startPos >= 0 && endPos >= 0) {
            String newFieldValue = text.substring(startPos, endPos);
            newFields.put(newFieldName, newFieldValue);
          }

          iterator.remove();
        }
      }
    }

    // add new fields
    if (newFields.size() > 0) {
      event.putAll(newFields);
    }

    return event;
  }

  @Override
  public Event parseNext() {
    boolean parse = true;

    Event fieldEvent = null;

    Event nextItem = null;
    while (parse) {
      int ev = 0;
      try {
        ev = parser.nextEvent();
      } catch (IOException | JSONParser.ParseException e) {
        throw new RuntimeException(e);
      }
      switch (ev) {
        case JSONParser.ARRAY_START:
          stack.push(JSONParser.ARRAY_START);
          break;
        case JSONParser.ARRAY_END:
          // BUG in Noggit parser not detecting start of array
          if (stack.peek() == JSONParser.ARRAY_START) {
            stack.pop();
          }
          break;
        case JSONParser.OBJECT_START:
          stack.push(JSONParser.OBJECT_START);
          switch (stack.size()) {
            case 2:
              nextItem = new Event();
              break;
            case 3:
              fieldEvent = new Event();
              break;
            default:
              break;
          }
          break;
        case JSONParser.OBJECT_END:
          stack.pop();
          switch (stack.size()) {
            case 1:
              parse = false;
              break;
            case 2:
              extractPositionalInfo(fieldEvent);
              nextItem.putAll(fieldEvent);
              fieldEvent = null;
              break;
            default:
              break;
          }
          break;
        case JSONParser.STRING:
        case JSONParser.NUMBER:
          try {
            Event event = null;
            switch (parser.getLevel()) {
              case 3:
                event = nextItem;
                break;
              case 5:
                event = fieldEvent;
                break;
              default:
                break;
            }
            handleCase(parser, event);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          break;
        case JSONParser.EOF:
          parse = false;
          break;
        default:
          break;
      }
    }

    if (nextItem != null) {
      nextItem = addPositionalInfo(nextItem);
    }

    return nextItem;
  }

  protected void handleCase(JSONParser parser, Event event) throws IOException {
    String key = parser.getString();
    int ev = parser.nextEvent();
    String fieldKey;
    String fieldValue;
    switch (key) {
      case FieldConstants.TIMESTAMP_FIELD:
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
          String transformedKey = StepParser.transformEventFieldKey(key);
          event.put(transformedKey, parseSingleFieldValue(ev));
        }
        break;
      default:
        break;
    }
  }

  protected Object parseSingleFieldValue(int ev) throws IOException {
    switch (ev) {
      case JSONParser.STRING:
        return parser.getString();
      case JSONParser.LONG:
        return parser.getLong();
      case JSONParser.NUMBER:
        return parser.getDouble();
      case JSONParser.BIGNUMBER:
        return parser.getNumberChars().toString();
      case JSONParser.BOOLEAN:
        return parser.getBoolean();
      case JSONParser.OBJECT_START:
        return (new ObjectBuilder(parser)).getObject();
      case JSONParser.ARRAY_START:
        return (new ObjectBuilder(parser)).getArray();
      case JSONParser.NULL:
        parser.getNull();
        return null;
      default:
        return parser.getString();
    }
  }
}