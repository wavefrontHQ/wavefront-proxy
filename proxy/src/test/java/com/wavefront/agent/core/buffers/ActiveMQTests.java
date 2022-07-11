package com.wavefront.agent.core.buffers;

import static org.junit.Assert.assertEquals;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.junit.Test;

public class ActiveMQTests {

  private static final int MENSAGES = 100;
  public static final String TEST_QUEUE = "test_queue";

  @Test
  public void ack() throws Throwable {
    Configuration config = new ConfigurationImpl();
    config.setName("test");
    config.setSecurityEnabled(false);
    config.setPersistenceEnabled(false);

    EmbeddedActiveMQ amq = new EmbeddedActiveMQ();
    config.addAcceptorConfiguration("in-vm", "vm://0");
    amq.setConfiguration(config);
    amq.start();

    ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://0");
    ClientSessionFactory factory = serverLocator.createSessionFactory();
    ClientSession session = factory.createSession();

    QueueConfiguration queue =
        new QueueConfiguration(TEST_QUEUE)
            .setAddress(TEST_QUEUE)
            .setRoutingType(RoutingType.ANYCAST);
    session.createQueue(queue);

    ClientProducer producer = session.createProducer(TEST_QUEUE);
    session.start();
    for (int i = 0; i < MENSAGES; i++) {
      ClientMessage message = session.createMessage(true);
      message.writeBodyBufferString("tururu");
      producer.send(message);
    }
    session.commit();

    ClientConsumer consumer = session.createConsumer(TEST_QUEUE);
    QueueControl queueControl =
        (QueueControl)
            amq.getActiveMQServer()
                .getManagementService()
                .getResource(ResourceNames.QUEUE + TEST_QUEUE);

    session.start();
    for (int i = 0; i < MENSAGES; i++) {
      ClientMessage msg = consumer.receive(100);
      if (i % 2 == 0) {
        msg.individualAcknowledge();
      }
    }
    session.commit();
    session.rollback();
    session.stop();

    assertEquals("", MENSAGES / 2, queueControl.countMessages());

    session.start();
    for (int i = 0; i < MENSAGES / 2; i++) {
      ClientMessage msg = consumer.receive(100);
      if (msg == null) break;
      msg.individualAcknowledge();
    }
    session.commit();
    session.close();
    assertEquals("", 0, queueControl.countMessages());

    amq.stop();
  }
}
