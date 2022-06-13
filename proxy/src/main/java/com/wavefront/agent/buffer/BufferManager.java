package com.wavefront.agent.buffer;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.common.Pair;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;

interface OnMsgFunction {
  void run(String msg) throws Exception;
}

public class BufferManager {
  private static final Logger logger = Logger.getLogger(BufferManager.class.getCanonicalName());

  private static EmbeddedActiveMQ embeddedMen;
  private static EmbeddedActiveMQ embeddedDisk;

  private static final Map<String, Pair<ClientSession, ClientProducer>> memoryProducer =
      new HashMap<>();
  private static final Map<String, Pair<ClientSession, ClientConsumer>> memoryConsumer =
      new HashMap<>();

  private static final Map<String, Gauge> mcMetrics = new HashMap<>();
  private static ConfigurationImpl config;

  public static void init() {
    try {
      config = new ConfigurationImpl();
      config.addAcceptorConfiguration("in-vm", "vm://0");
      config.setName("memory");
      config.setSecurityEnabled(false);
      config.setPersistenceEnabled(false);

      embeddedMen = new EmbeddedActiveMQ();
      embeddedMen.setConfiguration(config);
      embeddedMen.start();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "error creating memory buffer", e);
      System.exit(-1);
    }

    //		try {
    //			Configuration config = new ConfigurationImpl();
    //			config.addAcceptorConfiguration("in-vm", "vm://1");
    //			config.setName("disk");
    //			config.setSecurityEnabled(false);
    //			config.setPersistenceEnabled(true);
    //			embeddedDisk = new EmbeddedActiveMQ();
    //			embeddedDisk.setConfiguration(config);
    //			embeddedDisk.start();
    //		} catch (Exception e) {
    //			embeddedMen = null;
    //			logger.log(Level.SEVERE, "error creating disk buffer", e);
    //		}
  }

  public static void registerNewPort(String port) {
    QueueConfiguration queue =
        new QueueConfiguration(port + ".points")
            .setAddress(port)
            .setRoutingType(RoutingType.ANYCAST);
    QueueConfiguration queue_td =
        new QueueConfiguration(port + ".points.dl")
            .setAddress(port)
            .setRoutingType(RoutingType.ANYCAST);

    AddressSettings addrSetting = new AddressSettings();
    addrSetting.setMaxExpiryDelay(5000l);
    addrSetting.setMaxDeliveryAttempts(3); // TODO: config ?
    addrSetting.setDeadLetterAddress(
        SimpleString.toSimpleString(port + "::" + port + ".points.dl"));
    addrSetting.setExpiryAddress(SimpleString.toSimpleString(port + "::" + port + ".points.dl"));

    embeddedMen.getActiveMQServer().getAddressSettingsRepository().addMatch(port, addrSetting);

    try {
      ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://0");
      ClientSessionFactory factory = serverLocator.createSessionFactory();
      ClientSession session = factory.createSession();
      session.createQueue(queue);
      session.createQueue(queue_td);
    } catch (Exception e) {
      logger.log(Level.SEVERE, "error", e);
    }

    try {
      registerQueueMetrics(port);
    } catch (MalformedObjectNameException e) {
      logger.log(Level.SEVERE, "error", e);
    }
  }

  private static void registerQueueMetrics(String port) throws MalformedObjectNameException {
    ObjectName nameMen =
        new ObjectName(
            "org.apache.activemq.artemis:"
                + "broker=\"memory\","
                + "component=addresses,"
                + "address=\""
                + port
                + "\","
                + "subcomponent=queues,"
                + "routing-type=\"anycast\","
                + "queue=\""
                + port
                + ".points\"");
    Gauge mc =
        Metrics.newGauge(
            new MetricName("buffer.memory." + port, "", "MessageCount"),
            new Gauge<Integer>() {
              @Override
              public Integer value() {
                Long mc = null;
                try {
                  mc =
                      (Long)
                          embeddedMen
                              .getActiveMQServer()
                              .getMBeanServer()
                              .getAttribute(nameMen, "MessageCount");
                } catch (Exception e) {
                  e.printStackTrace();
                  return 0;
                }
                return mc.intValue(); // datum.size();
              }
            });
    mcMetrics.put(port, mc);
  }

  public static void sendMsg(String port, List<String> strPoints) {
    String key = port + "." + Thread.currentThread().getName();
    Pair<ClientSession, ClientProducer> mqCtx =
        memoryProducer.computeIfAbsent(
            key,
            s -> {
              try {
                ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://0");
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                ClientSession session =
                    factory.createSession(
                        false,
                        false); // 1st false mean we commit msg.send on only on session.commit
                ClientProducer producer = session.createProducer(port + "::" + port + ".points");
                return new Pair<>(session, producer);
              } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
              }
              return null;
            });

    ClientSession session = mqCtx._1;
    ClientProducer producer = mqCtx._2;
    try {
      session.start();
      for (String s : strPoints) {
        ClientMessage message = session.createMessage(false);
        message.writeBodyBufferString(s);
        producer.send(message);
      }
      session.commit();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "error", e);
    }
  }

  @VisibleForTesting
  static Gauge<Long> getMcGauge(String port) {
    return mcMetrics.get(port);
  }

  public static void onMsg(String port, OnMsgFunction func) {
    String key = port + "." + Thread.currentThread().getName();
    Pair<ClientSession, ClientConsumer> mqCtx =
        memoryConsumer.computeIfAbsent(
            key,
            s -> {
              try {
                ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://0");
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                ClientSession session =
                    factory.createSession(
                        false,
                        false); // 2sd false means that we send msg.ack only on session.commit
                ClientConsumer consumer = session.createConsumer(port + "::" + port + ".points");
                return new Pair<>(session, consumer);
              } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
              }
              return null;
            });

    ClientSession session = mqCtx._1;
    ClientConsumer consumer = mqCtx._2;
    try {
      session.start();
      ClientMessage msg = consumer.receiveImmediate();
      if (msg != null) {
        try {
          msg.acknowledge();
          func.run(msg.getReadOnlyBodyBuffer().readString());
          session.commit();
        } catch (Exception e) {
          System.out.println("--> " + msg.getDeliveryCount());
          session.rollback();
        }
      }
    } catch (ActiveMQException e) {
      logger.log(Level.SEVERE, "error", e);
    } finally {
      try {
        session.stop();
      } catch (ActiveMQException e) {
        logger.log(Level.SEVERE, "error", e);
      }
    }
  }
}
