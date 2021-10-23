package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;

import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 10/21/21 2:47 PM
 */
public class SimpleJsonData {

  public static Object sample1EventList = createSample1EventList();

  public static Object sample2EventList = createSample2EventList();

  public static Object sample5EventList = createSample5EventList();

  public static Object sample6EventList = createSample6EventList();

  public static Object sample8EventList = createSample8EventList();

  public static Object sample9EventList = createSample9EventList();

  public static Object sample10EventList = createSample10EventList();

  public static String getSimpleJsonSample1() throws IOException {
    return TestData.readFile("/ingestion-data/SimpleJsonSample1.json");
  }

  public static String getSimpleJsonSample2() throws IOException {
    return TestData.readFile("/ingestion-data/SimpleJsonSample2.json");
  }

  public static String getSimpleJsonSample3() throws IOException {
    return TestData.readFile("/ingestion-data/SimpleJsonSample3.json");
  }

  public static String getSimpleJsonSample4() throws IOException {
    return TestData.readFile("/ingestion-data/SimpleJsonSample4.json");
  }

  public static String getSimpleJsonSample5() throws IOException {
    return TestData.readFile("/ingestion-data/SimpleJsonSample5.json");
  }

  public static String getSimpleJsonSample6() throws IOException {
    return TestData.readFile("/ingestion-data/SimpleJsonSample6.json");
  }

  public static String getSimpleJsonSample7() throws IOException {
    return TestData.readFile("/ingestion-data/SimpleJsonSample7.json");
  }

  public static String getSimpleJsonSample8() throws IOException {
    return TestData.readFile("/ingestion-data/SimpleJsonSample8.json");
  }

  public static String getSimpleJsonSample9() throws IOException {
    return TestData.readFile("/ingestion-data/SimpleJsonSample9.json");
  }

  public static String getSimpleJsonSample10() throws IOException {
    return TestData.readFile("/ingestion-data/SimpleJsonSample10.json");
  }

  private static List<Event> createSample1EventList() {
    List<Event> eventList = new ArrayList<Event>();
    Event event = new Event();
    event.put("hostname", "w3-hs1-050301.eng.vmware.com");
    event.put("appname", "Vpxa");
    event.put("source", "prome-1s-dhcp-133-165.eng.vmware.com");
    eventList.add(event);

    Event event2 = new Event();
    event2.put("hostname", "vm1.eng.vmware.com");
    event2.put("appname", "Vpxa");
    event2.put("source", "prome-1s-dhcp-vm1.eng.vmware.com");
    eventList.add(event2);
    return eventList;
  }

  private static List<Event> createSample2EventList() {
    List<Event> eventList = new ArrayList<Event>();
    Event event = new Event();
    event.put("hostname", "w3-hs1-050301.eng.vmware.com");
    event.put("appname", "Vpxa");
    event.put("source", "prome-1s-dhcp-133-165.eng.vmware.com");
    eventList.add(event);
    return eventList;
  }

  private static List<Event> createSample5EventList() {
    List<Event> eventList = new ArrayList<Event>();
    Event event = new Event();
    event.put("hostname", "w3-hs1-050301.eng.vmware.com");
    event.put("appname", "Vpxa");
    eventList.add(event);

    Event event2 = new Event();
    event2.put("hostname", "vm1.eng.vmware.com");
    event2.put("appname", "Vpxa");
    eventList.add(event2);
    return eventList;
  }

  private static List<Event> createSample6EventList() {
    List<Event> eventList = new ArrayList<Event>();
    Event event = new Event();
    event.put("hostname", "w3-hs1-050301.eng.vmware.com");
    event.put("appname", "Vpxa");
    event.put("text", null);
    eventList.add(event);

    Event event2 = new Event();
    event2.put("hostname", "vm1.eng.vmware.com");
    event2.put("appname", "Vpxa");
    event2.put("text", "");
    eventList.add(event2);
    return eventList;
  }

  private static List<Event> createSample8EventList() {
    List<Event> eventList = new ArrayList<Event>();
    Event event = new Event();
    event.put("text", "{\"eventVersion\":\"1.05\",\"userIdentity\":{\"type\":\"IAMUser\",\"userName\":\"flands\"}" +
        ",\"eventTime\":\"2018-06-14T17:17:55Z\",\"eventSource\":\"ec2.amazonaws.com\",\"requestParameters\":" +
        "{\"availabilityZoneSet\":{}},\"responseElements\":null,\"recipientAccountId\":\"792677813823\"}");
    event.put("eventVersion", "1.05");
    event.put("userIdentity_type", "IAMUser");
    event.put("userIdentity_userName", "flands");
    event.put("eventTime", "2018-06-14T17:17:55Z");
    event.put("eventSource", "ec2.amazonaws.com");
    event.put("responseElements", null);
    event.put("recipientAccountId", "792677813823");
    eventList.add(event);
    return eventList;
  }

  private static List<Event> createSample9EventList() {
    List<Event> eventList = new ArrayList<Event>();
    Event event = new Event();
    event.put("object_kind", "push");
    event.put("project_visibility_level", 0L);
    event.put("commits_id", "1\n2");
    event.put("commits_message", "Update Catalan translation to e38cb41.\nfixed readme");
    event.put("previous_commits", "[\"first_commit\",\"second_commit\"]");
    eventList.add(event);
    return eventList;
  }

  private static List<Event> createSample10EventList() {
    List<Event> eventList = new ArrayList<Event>();
    Event event = new Event();
    event.put("messages_event", "incident.annotate\nincident.annotate");
    event.put("messages_log_entries_type", "create_log_entry\ncreate_log_entry");
    event.put("messages_log_entries_channel_type", "note\nnote");
    event.put("messages_log_entries_channel_details_omitted", "false\nfalse");
    event.put("messages_log_entries_channel_impacted_services_id", "P3PCEK5\nP3PCEK5\nP3PCEK5\nP3PCEK5");
    event.put("messages_log_entries_channel_impacted_services_type", "service_reference\nservice_" +
        "reference\nservice_reference\nservice_reference");
    eventList.add(event);
    return eventList;
  }
}