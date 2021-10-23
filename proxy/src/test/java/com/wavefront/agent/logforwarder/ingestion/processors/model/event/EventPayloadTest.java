package com.wavefront.agent.logforwarder.ingestion.processors.model.event;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 10/22/21 12:13 PM
 */
public class EventPayloadTest {
  @Test
  public void testPayloadSplitter() {
    Event event1 = new Event();
    event1.put("text", "2017-04-20T19:43:25.1479 w3-hs1-050301.eng.vmware.com " +
        "Vpxa[78475]: text foo-bar verbose foo-bar "
        + "text4 verbose vpxa[6CAFEB70] [Originator@6876 sub=VpxaHalCnxHostagent " +
        "opID=WFU-4a548e30] [WaitForUpdatesStarted] Received callback");

    Event event2 = new Event();
    event2.put("text", "2017-04-20T19:43:25.1479 w3-hs1-050301.eng.vmware.com " +
        "Vpxa[78475]: text foo-bar verbose foo-bar "
        + "text4 verbose vpxa[6CAFEB70] [Originator@6876 sub=VpxaHalCnxHostagent " +
        "opID=WFU-4a548e30] [WaitForUpdatesInProgress] Received callback");

    Event event3 = new Event();
    event3.put("text", "2017-04-20T19:43:25.1479 w3-hs1-050301.eng.vmware.com " +
        "Vpxa[78475]: text foo-bar verbose foo-bar "
        + "text4 verbose vpxa[6CAFEB70] [Originator@6876 sub=VpxaHalCnxHostagent " +
        "opID=WFU-4a548e30] [WaitForUpdatesCompleted] Received callback");

    EventBatch events = new EventBatch();
    events.add(event1);
    events.add(event2);
    events.add(event3);

    EventPayload eventPayload = new EventPayload("tenant-1", "tenant" + "-org",
        EventPayload.PayloadType.LOGS, events, -1,
        new HashMap<>(), false);
    eventPayload.requestHeaders = new HashMap<>();
    eventPayload.payloadSizeInBytes = 1;

    List<EventPayload> splitPayloads = eventPayload.split(2);
    Assert.assertEquals(2, splitPayloads.size());
    Assert.assertEquals(2, splitPayloads.get(0).batch.size());
    Assert.assertEquals(1, splitPayloads.get(1).batch.size());
  }
}
