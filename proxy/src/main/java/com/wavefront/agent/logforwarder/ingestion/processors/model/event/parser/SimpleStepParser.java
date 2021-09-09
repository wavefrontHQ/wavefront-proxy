package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;

import com.vmware.ingestion.transformers.CFAPI;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;

import org.noggit.JSONParser;
import org.noggit.JSONUtil;
import org.noggit.ObjectBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 1:00 PM
 */
public class SimpleStepParser implements StepParser {
  protected JSONParser parser;
  protected Stack<Integer> stack = new Stack<>();

  public SimpleStepParser(JSONParser parser) {
    this.parser = parser;
  }

  @Override
  public Event parseNext() {
    boolean parse = true;

    Event newNextItem = null;
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
          if (stack.size() == 0) {
            parse = false;
          }
          break;
        case JSONParser.OBJECT_START:
          // simple json was not contained in an array
          if (stack.size() == 0) {
            stack.push(JSONParser.ARRAY_START);
          }
          stack.push(JSONParser.OBJECT_START);
          if (stack.size() == 2) {
            newNextItem = new Event();
          }
          break;
        case JSONParser.OBJECT_END:
          stack.pop();
          if (stack.size() == 1) {
            parse = false;
          }
          break;
        case JSONParser.STRING:
        case JSONParser.NUMBER:
          try {
            handleCase(parser, newNextItem);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          break;
        case JSONParser.EOF:
          // simple json was not contained in an array
          if (stack.peek() == JSONParser.ARRAY_START) {
            stack.pop();
          }
          parse = false;
          break;
        default:
          break;
      }
    }
    return newNextItem;
  }

  @SuppressWarnings("unchecked")
  private void handleCase(JSONParser parser, Event event) throws IOException {
    String k = parser.getString();
    int ev = parser.nextEvent();
    Object v = parseSingleFieldValue(ev, k);
    String transformedKey = StepParser.transformEventFieldKey(k);
    if (v != null && v.getClass() == ArrayList.class) {
      listParser(event, transformedKey, (ArrayList) v);
    } else if (v == null || v.getClass() != LinkedHashMap.class) {
      event.put(transformedKey, v);
    } else {
      objectParser(event, transformedKey, (Map<String, Object>) v);
    }
  }

  private Object parseSingleFieldValue(int ev, String fieldName) throws IOException {
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

  private void objectParser(Event event, String currentPath, Map<String, Object> inputMap) {
    Map<String, Object> flattenedMap = new LinkedHashMap<>();
    if (currentPath.equals(CFAPI.TEXT_FIELD)) {
      event.put(CFAPI.TEXT_FIELD, JSONUtil.toJSON(inputMap, -1)); // Retain Text Field
      addKeys("", inputMap, flattenedMap);
    } else {
      addKeys(currentPath, inputMap, flattenedMap);
    }
    flattenedMap.forEach((key, value) -> {
      if (event != null && !event.containsKey(key)) {
        event.put(key, value);
      }
    });
  }

  @SuppressWarnings("unchecked")
  private void listParser(Event event, String currentPath, ArrayList inputList) {
    inputList.forEach(listItem -> {
      if (listItem.getClass() != LinkedHashMap.class) {
        event.put(currentPath, JSONUtil.toJSON(inputList, -1));
      } else {
        Map<String, Object> listItemMap = (LinkedHashMap) listItem;
        addKeys(currentPath, listItemMap, event);
      }
    });
  }

  @SuppressWarnings("unchecked")
  private void addKeys(String currentPath, Map<String, Object> inputMap, Map<String, Object> outputMap) {
    inputMap.forEach((key, value) -> {
      String pathPrefix = currentPath.isEmpty() ? "" : currentPath + "_";
      if (value == null) {
        outputMap.put(pathPrefix + key, null);
      } else if (value.getClass() == ArrayList.class) {
        outputMap.put(pathPrefix + key, JSONUtil.toJSON(value, -1));
      } else if (value.getClass() == LinkedHashMap.class) {
        addKeys(pathPrefix + key, (Map<String, Object>) value, outputMap);
      } else {
        outputMap.put(pathPrefix + key, value);
      }
    });
  }

  @SuppressWarnings("unchecked")
  private void addKeys(String currentPath, Map<String, Object> inputMap, Event event) {
    inputMap.forEach((listItemMapKey, listItemMapValue) -> {
      String currentKey = currentPath + "_" + listItemMapKey;
      if (listItemMapValue == null) {
        event.put(currentKey, null);
      } else if (listItemMapValue.getClass() == ArrayList.class) {
        listParser(event, currentKey, (ArrayList) listItemMapValue);
      } else if (listItemMapValue.getClass() != LinkedHashMap.class) {
        if (event.containsKey(currentKey)) {
          event.put(currentKey, event.get(currentKey) + "\n" + listItemMapValue);
        } else {
          event.put(currentKey, listItemMapValue);
        }
      } else {
        addKeys(currentKey, (Map<String, Object>) listItemMapValue, event);
      }
    });
  }
}

