package com.wavefront.agent.buffer;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.common.Pair;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;

class BufferActiveMQ implements Buffer {
  private static final Logger log = Logger.getLogger(BuffersManager.class.getCanonicalName());

  private final EmbeddedActiveMQ embeddedMen;

  private final Map<String, Pair<ClientSession, ClientProducer>> producers = new HashMap<>();
  private final Map<String, Pair<ClientSession, ClientConsumer>> consumers = new HashMap<>();

  private final Map<String, Gauge<Long>> mcMetrics = new HashMap<>();
  private final String name;
  private final int level;

  public BufferActiveMQ(int level, String name, boolean persistenceEnabled, String buffer) {
    this.level = level;
    this.name = name;

    log.info("-> buffer:'" + buffer + "'");

    Configuration config = new ConfigurationImpl();
    config.setName(name);
    config.setSecurityEnabled(false);
    config.setPersistenceEnabled(persistenceEnabled);
    config.setJournalDirectory(buffer + "/journal");
    config.setBindingsDirectory(buffer + "/bindings");
    config.setLargeMessagesDirectory(buffer + "/largemessages");
    config.setCreateBindingsDir(true);
    config.setCreateJournalDir(true);
    config.setMessageExpiryScanPeriod(persistenceEnabled ? 0 : 1_000);

    embeddedMen = new EmbeddedActiveMQ();

    try {
      config.addAcceptorConfiguration("in-vm", "vm://" + level);
      embeddedMen.setConfiguration(config);
      embeddedMen.start();
    } catch (Exception e) {
      log.log(Level.SEVERE, "error creating buffer", e);
      System.exit(-1);
    }
  }

  public void registerNewPort(String port) {
    QueueConfiguration queue =
        new QueueConfiguration(name + "." + port + ".points")
            .setAddress(port)
            .setRoutingType(RoutingType.ANYCAST);
    QueueConfiguration queue_dl =
        new QueueConfiguration(name + "." + port + ".points.dl")
            .setAddress(port)
            .setRoutingType(RoutingType.ANYCAST);

    try {
      ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://" + level);
      ClientSessionFactory factory = serverLocator.createSessionFactory();
      ClientSession session = factory.createSession();
      ClientSession.QueueQuery q = session.queueQuery(queue.getName());
      if (!q.isExists()) {
        session.createQueue(queue);
        session.createQueue(queue_dl);
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }

    try {
      registerQueueMetrics(port);
    } catch (MalformedObjectNameException e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }
  }

  void registerQueueMetrics(String port) throws MalformedObjectNameException {
    ObjectName nameMen =
        new ObjectName(
            "org.apache.activemq.artemis:"
                + "broker=\""
                + name
                + "\","
                + "component=addresses,"
                + "address=\""
                + port
                + "\","
                + "subcomponent=queues,"
                + "routing-type=\"anycast\","
                + "queue=\""
                + name
                + "."
                + port
                + ".points\"");
    Gauge<Long> mc =
        Metrics.newGauge(
            new MetricName("buffer." + name + "." + port, "", "MessageCount"),
            new Gauge<Long>() {
              @Override
              public Long value() {
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
                  return 0L;
                }
                return mc; // datum.size();
              }
            });
    mcMetrics.put(port, mc);
  }

  public void createBridge(String port, int level) {
    AddressSettings addrSetting = new AddressSettings();
    addrSetting.setMaxExpiryDelay(5000l); // TODO: config ?
    addrSetting.setMaxDeliveryAttempts(3); // TODO: config ?
    addrSetting.setDeadLetterAddress(
        SimpleString.toSimpleString(port + "::" + name + "." + port + ".points.dl"));
    addrSetting.setExpiryAddress(
        SimpleString.toSimpleString(port + "::" + name + "." + port + ".points.dl"));

    embeddedMen.getActiveMQServer().getAddressSettingsRepository().addMatch(port, addrSetting);

    BridgeConfiguration bridge = new BridgeConfiguration();
    bridge.setName(port + ".to.l" + level);
    bridge.setQueueName(port + "::" + name + "." + port + ".points.dl");
    bridge.setForwardingAddress(port + "::disk." + port + ".points");
    bridge.setStaticConnectors(Collections.singletonList("to.level_" + level));
    try {
      embeddedMen
          .getActiveMQServer()
          .getConfiguration()
          .addConnectorConfiguration("to.level_" + (level), "vm://" + (level));
      embeddedMen.getActiveMQServer().deployBridge(bridge);
    } catch (Exception e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }
  }

  public void sendMsg(String port, List<String> strPoints) {
    String key = port + "." + Thread.currentThread().getName();
    Pair<ClientSession, ClientProducer> mqCtx =
        producers.computeIfAbsent(
            key,
            s -> {
              try {
                ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://" + level);
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                ClientSession session =
                    factory.createSession(
                        false,
                        false); // 1st false mean we commit msg.send on only on session.commit
                ClientProducer producer =
                    session.createProducer(port + "::" + name + "." + port + ".points");
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
        ClientMessage message = session.createMessage(true);
        message.writeBodyBufferString(s);
        producer.send(message);
      }
      session.commit();
    } catch (Exception e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }
  }

  @VisibleForTesting
  public Gauge<Long> getMcGauge(String port) {
    return mcMetrics.get(port);
  }

  public void onMsg(String port, OnMsgFunction func) {}

  public void onMsgBatch(String port, int batchSize, OnMsgFunction func) {
    String key = port + "." + Thread.currentThread().getName();
    Pair<ClientSession, ClientConsumer> mqCtx =
        consumers.computeIfAbsent(
            key,
            s -> {
              try {
                ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://" + level);
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                ClientSession session =
                    factory.createSession(
                        false,
                        false); // 2sd false means that we send msg.ack only on session.commit
                ClientConsumer consumer =
                    session.createConsumer(port + "::" + name + "." + port + ".points");
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
      List<String> batch = new ArrayList<>(batchSize);
      while (batch.size() < batchSize) {
        ClientMessage msg = consumer.receive(10);
        if (msg != null) {
          msg.acknowledge();
          batch.add(msg.getReadOnlyBodyBuffer().readString());
        } else {
          break;
        }
      }

      try {
        func.run(batch);
        session.commit();
      } catch (Exception e) {
        session.rollback();
      }

    } catch (ActiveMQException e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    } finally {
      try {
        session.stop();
      } catch (ActiveMQException e) {
        log.log(Level.SEVERE, "error", e);
        System.exit(-1);
      }
    }
  }
}
