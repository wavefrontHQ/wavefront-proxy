package com.wavefront.agent.buffer.activeMQ;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.buffer.Buffer;
import com.wavefront.agent.buffer.BuffersManager;
import com.wavefront.agent.buffer.OnMsgFunction;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.common.Pair;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.MBeanServer;
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

abstract class BufferActiveMQ implements Buffer {
  private static final Logger log = Logger.getLogger(BuffersManager.class.getCanonicalName());

  private final EmbeddedActiveMQ amq;

  private final Map<String, Pair<ClientSession, ClientProducer>> producers =
      new ConcurrentHashMap<>();
  private final Map<String, Pair<ClientSession, ClientConsumer>> consumers =
      new ConcurrentHashMap<>();

  private final Map<String, Gauge<Long>> mcMetrics = new HashMap<>();
  private final String name;
  private final int level;
  private final MBeanServer mbServer;

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

    amq = new EmbeddedActiveMQ();

    try {
      config.addAcceptorConfiguration("in-vm", "vm://" + level);
      amq.setConfiguration(config);
      amq.start();
    } catch (Exception e) {
      log.log(Level.SEVERE, "error creating buffer", e);
      System.exit(-1);
    }

    mbServer = amq.getActiveMQServer().getMBeanServer();
  }

  public void registerNewHandlerKey(HandlerKey key) {
    QueueConfiguration queue =
        new QueueConfiguration(key.getQueue())
            .setAddress(key.getPort())
            .setRoutingType(RoutingType.ANYCAST);
    QueueConfiguration queue_dl =
        new QueueConfiguration(key.getQueue() + ".dl")
            .setAddress(key.getPort())
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
      registerQueueMetrics(key);
    } catch (MalformedObjectNameException e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }
  }

  void registerQueueMetrics(HandlerKey key) throws MalformedObjectNameException {
    ObjectName nameMen =
        new ObjectName(
            "org.apache.activemq.artemis:"
                + "broker=\""
                + name
                + "\","
                + "component=addresses,"
                + "address=\""
                + key.getPort()
                + "\","
                + "subcomponent=queues,"
                + "routing-type=\"anycast\","
                + "queue=\""
                + key.getQueue()
                + "\"");
    Gauge<Long> mc =
        Metrics.newGauge(
            new MetricName("buffer." + name + "." + key.getQueue(), "", "MessageCount"),
            new Gauge<Long>() {
              @Override
              public Long value() {
                Long mc = null;
                try {
                  mc = (Long) mbServer.getAttribute(nameMen, "MessageCount");
                } catch (Exception e) {
                  e.printStackTrace();
                  return 0L;
                }
                return mc; // datum.size();
              }
            });
    mcMetrics.put(key.getQueue(), mc);
  }

  public void createBridge(String addr, String queue, int level) {
    AddressSettings addrSetting = new AddressSettings();
    addrSetting.setMaxExpiryDelay(5000l); // TODO: config ?
    addrSetting.setMaxDeliveryAttempts(3); // TODO: config ?
    addrSetting.setDeadLetterAddress(SimpleString.toSimpleString(addr + "::" + queue + ".dl"));
    addrSetting.setExpiryAddress(SimpleString.toSimpleString(addr + "::" + queue + ".dl"));

    amq.getActiveMQServer().getAddressSettingsRepository().addMatch(addr, addrSetting);

    BridgeConfiguration bridge = new BridgeConfiguration();
    bridge.setName(addr + ".to.l" + level);
    bridge.setQueueName(addr + "::" + queue + ".points.dl");
    bridge.setForwardingAddress(addr + "::" + queue + ".points");
    bridge.setStaticConnectors(Collections.singletonList("to.level_" + level));
    try {
      amq.getActiveMQServer()
          .getConfiguration()
          .addConnectorConfiguration("to.level_" + (level), "vm://" + (level));
      amq.getActiveMQServer().deployBridge(bridge);
    } catch (Exception e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }
  }

  @Override
  public void sendMsg(HandlerKey key, List<String> strPoints) {
    String sessionKey = key.getQueue() + "." + Thread.currentThread().getName();
    Pair<ClientSession, ClientProducer> mqCtx =
        producers.computeIfAbsent(
            sessionKey,
            s -> {
              try {
                ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://" + level);
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                // 1st false mean we commit msg.send on only on session.commit
                ClientSession session = factory.createSession(false, false);
                ClientProducer producer =
                    session.createProducer(key.getPort() + "::" + key.getQueue());
                return new Pair<>(session, producer);
              } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
              }
              return null;
            });

    // TODO: check if session still valid
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

  @Override
  @VisibleForTesting
  public Gauge<Long> getMcGauge(HandlerKey handlerKey) {
    return mcMetrics.get(handlerKey.getQueue());
  }

  @Override
  public void shutdown() {
    try {
      amq.stop();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  @Override
  public void onMsg(HandlerKey key, OnMsgFunction func) {}

  @Override
  public void onMsgBatch(
      HandlerKey key, int batchSize, RecyclableRateLimiter rateLimiter, OnMsgFunction func) {
    String sessionKey = key.getQueue() + "." + Thread.currentThread().getName();
    Pair<ClientSession, ClientConsumer> mqCtx =
        consumers.computeIfAbsent(
            sessionKey,
            s -> {
              try {
                ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://" + level);
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                // 2sd false means that we send msg.ack only on session.commit
                ClientSession session = factory.createSession(false, false);
                ClientConsumer consumer =
                    session.createConsumer(key.getPort() + "::" + key.getQueue());
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
      while ((batch.size() < batchSize) && (rateLimiter.tryAcquire())) {
        ClientMessage msg = consumer.receive(10);
        if (msg != null) {
          msg.acknowledge();
          batch.add(msg.getReadOnlyBodyBuffer().readString());
        } else {
          break;
        }
      }

      try {
        if (batch.size() > 0) {
          func.run(batch);
        }
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
