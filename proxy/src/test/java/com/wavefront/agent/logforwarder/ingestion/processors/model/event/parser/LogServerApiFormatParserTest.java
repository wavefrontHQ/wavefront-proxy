package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;

import com.vmware.xenon.common.Utils;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api.StructureParser;

import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 10/21/21 3:29 PM
 */
public class LogServerApiFormatParserTest {
  @Test
  public void testParseIngestionAPIJson() throws IOException {
    String cfapiJson = getLogIngestionServerSample1Json();
    StructureParser parser = new StructureFactory().getParser(StructureFactory.Structure.SERVER_CFAPI);
    Iterable<Event> events = parser.parse(new StringReader(cfapiJson));
    List<Event> eventList = new ArrayList<Event>();
    for (Event event : events) {
      eventList.add(event);
    }

    assertEquals(Utils.toJson(createLiServerSample()),
        Utils.toJson(eventList));
  }

  public static String getLogIngestionServerSample1Json() throws IOException {
    return TestData.readFile("/ingestion-data/LogInsightServerCfapiSample1.json");
  }

  public static List<Event> createLiServerSample() {
    List<Event> eventList = new ArrayList<Event>();
    Event event = new Event();
    event.put("text",
        "2017-04-20T19:43:25.1479 w3-hs1-050301.eng.vmware.com Vpxa[78475]: " +
            "verbose vpxa[6CAFEB70] [Originator@6876 sub=VpxaHalCnxHostagent " +
            "opID=WFU-4a548e30] [WaitForUpdatesDone] Received callback");
    event.put("hostname", "w3-hs1-050301.eng.vmware.com");
    event.put("appname", "Vpxa");
    event.put("source", "prome-1s-dhcp-133-165.eng.vmware.com");
    event.put("field_with_integer_value", 1L);
    event.put("field_with_double_value", 1.11);
    event.put("field_with_boolean_value", Boolean.FALSE);
    event.put("timestamp", 1491777731145L);
    eventList.add(event);
    return eventList;
  }

  private static List<Event> createSample1EventList() {
    List<Event> eventList = new ArrayList<Event>();
    Event event = new Event();
    event.put("text",
        "2017-04-20T19:43:25.1479 w3-hs1-050301.eng.vmware.com Vpxa[78475]: " +
            "verbose vpxa[6CAFEB70] [Originator@6876 sub=VpxaHalCnxHostagent " +
            "opID=WFU-4a548e30] [WaitForUpdatesDone] Received callback");
    event.put("hostname", "w3-hs1-050301.eng.vmware.com");
    event.put("appname", "Vpxa");
    event.put("source", "prome-1s-dhcp-133-165.eng.vmware.com");
    event.put("field_with_integer_value", 1L);
    event.put("field_with_double_value", 1.11);
    event.put("field_with_boolean_value", Boolean.FALSE);
    event.put("timestamp", 1491777731145L);
    eventList.add(event);
    return eventList;
  }
}
