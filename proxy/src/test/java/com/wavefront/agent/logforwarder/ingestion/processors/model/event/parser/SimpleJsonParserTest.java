package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;

import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api.StructureParser;
import org.junit.Test;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import static org.junit.Assert.assertEquals;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 10/21/21 2:46 PM
 */
public class SimpleJsonParserTest {
  @Test
  public void testParseSimpleJson3() throws IOException {
    String cfapiJson = SimpleJsonData.getSimpleJsonSample3();
    StructureParser parser = new SimpleJsonFormatParser();
    Iterable<Event> events = parser.parse(new StringReader(cfapiJson));
    List<Event> eventList = new ArrayList<Event>();
    for (Iterator<Event> iterator = events.iterator(); iterator.hasNext(); ) {
      eventList.add(iterator.next());
    }

    assertEquals(eventList.size(), 2);
    assertEquals(eventList.get(0).get("timestamp"), 1483667347951L);
    assertEquals(eventList.get(1).get("timestamp"), 1483667347952L);

    assertEquals(eventList.get(0).get("parse_failed"), true);
    assertEquals(eventList.get(1).get("parse_failed"), false);
  }

  @Test
  public void testParseCloudTrailJson() throws IOException {
    String messageSample = TestData.getCloudTrailLogMessageSample1();
    StructureParser parser = new SimpleJsonFormatParser();
    Iterable<Event> events = parser.parse(new StringReader(messageSample));
    int eventCount = 0;
    for (Event event : events) {
      eventCount++;
      assertEquals("ec2.amazonaws.com", event.get("eventSource"));
    }
    assertEquals(4, eventCount);
  }

  @Test
  public void testParseSimpleJson4() throws IOException {
    String simpleJson = SimpleJsonData.getSimpleJsonSample4();
    StructureParser parser = new SimpleJsonFormatParser();
    Iterable<Event> events = parser.parse(new StringReader(simpleJson));
    List<Event> eventList = new ArrayList<Event>();
    for (Event event : events) {
      eventList.add(event);
    }

    assertEquals(2, eventList.size());
    assertEquals("[1,2,3,4]", eventList.get(0).get("array1"));

    String expectedArray2 = "[1,2,{\"someObject\":{\"filterSet\":{\"items\":[{\"value\":\"vpc-04e3326d\"}]}}}]";
    assertEquals(expectedArray2, eventList.get(0).get("array2"));
    assertEquals(null, eventList.get(0).get("nullField"));
  }

  @Test
  public void testParseSimpleJson1() throws IOException {
    String simpleJson = SimpleJsonData.getSimpleJsonSample1();
    StructureParser parser = new SimpleJsonFormatParser();
    Iterable<Event> events = parser.parse(new StringReader(simpleJson));
    List<Event> eventList = new ArrayList<Event>();
    for (Event event : events) {
      eventList.add(event);
    }
    assertEquals(SimpleJsonData.sample1EventList, eventList);
  }

  @Test
  public void testParseSimpleJson2() throws IOException {
    String simpleJson = SimpleJsonData.getSimpleJsonSample2();
    StructureParser parser = new SimpleJsonFormatParser();
    Iterable<Event> events = parser.parse(new StringReader(simpleJson));
    List<Event> eventList = new ArrayList<Event>();
    for (Event event : events) {
      eventList.add(event);
    }
    assertEquals(SimpleJsonData.sample2EventList, eventList);
  }

  @Test
  public void testParseWithOutTextField() throws IOException {
    String simpleJson = SimpleJsonData.getSimpleJsonSample5();
    StructureParser parser = new SimpleJsonFormatParser();
    Iterable<Event> events = parser.parse(new StringReader(simpleJson));
    List<Event> eventList = new ArrayList<Event>();
    for (Event event : events) {
      eventList.add(event);
    }
    assertEquals(SimpleJsonData.sample5EventList, eventList);
  }

  @Test
  public void testParseWithTextFieldEmpty() throws IOException {
    String simpleJson = SimpleJsonData.getSimpleJsonSample6();
    StructureParser parser = new SimpleJsonFormatParser();
    Iterable<Event> events = parser.parse(new StringReader(simpleJson));
    List<Event> eventList = new ArrayList<Event>();
    for (Event event : events) {
      eventList.add(event);
    }
    assertEquals(SimpleJsonData.sample6EventList, eventList);
  }

  @Test
  public void testFlattenWithValidJson() throws IOException {
    String simpleJson = SimpleJsonData.getSimpleJsonSample7();
    StructureParser parser = new SimpleJsonFormatParser();
    Iterable<Event> events = parser.parse(new StringReader(simpleJson));
    List<Event> eventList = new ArrayList<Event>();
    for (Event event : events) {
      eventList.add(event);
    }
    assertEquals(16, eventList.get(0).size());
  }

//  @Test TODO
  public void testAlreadyFlattenJson() throws IOException {
    String simpleJson = SimpleJsonData.getSimpleJsonSample8();
    StructureParser parser = new SimpleJsonFormatParser();
    Iterable<Event> events = parser.parse(new StringReader(simpleJson));
    List<Event> eventList = new ArrayList<Event>();
    for (Event event : events) {
      eventList.add(event);
    }
    assertEquals(SimpleJsonData.sample8EventList, eventList);
  }

//  @Test TODO
  public void testListFlattener() throws IOException {
    String simpleJson = SimpleJsonData.getSimpleJsonSample9();
    StructureParser parser = new SimpleJsonFormatParser();
    Iterable<Event> events = parser.parse(new StringReader(simpleJson));
    List<Event> eventList = new ArrayList<Event>();
    for (Event event : events) {
      eventList.add(event);
    }
    assertEquals(SimpleJsonData.sample9EventList, eventList);
  }

//  @Test TODO
  public void testRecursiveListFlattener() throws IOException {
    String simpleJson = SimpleJsonData.getSimpleJsonSample10();
    StructureParser parser = new SimpleJsonFormatParser();
    Iterable<Event> events = parser.parse(new StringReader(simpleJson));
    List<Event> eventList = new ArrayList<Event>();
    for (Event event : events) {
      eventList.add(event);
    }
    assertEquals(SimpleJsonData.sample10EventList, eventList);
  }
}
