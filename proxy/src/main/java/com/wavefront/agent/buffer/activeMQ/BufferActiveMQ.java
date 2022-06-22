package com.wavefront.agent.buffer.activeMQ;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.buffer.*;
import com.wavefront.common.Pair;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.jetbrains.annotations.TestOnly;

public abstract class BufferActiveMQ implements Buffer {
  private static final Logger log = Logger.getLogger(BuffersManager.class.getCanonicalName());

  private final EmbeddedActiveMQ amq;

  private final Map<String, Pair<ClientSession, ClientProducer>> producers =
      new ConcurrentHashMap<>();
  private final Map<String, Pair<ClientSession, ClientConsumer>> consumers =
      new ConcurrentHashMap<>();

  private final Map<String, Gauge<Long>> mcMetrics = new HashMap<>();
  private final Map<String, Histogram> msMetrics = new HashMap<>();
  private final String name;
  @org.jetbrains.annotations.NotNull private final BufferConfig cfg;
  private final int level;
  private final MBeanServer mbServer;

  public BufferActiveMQ(int level, String name, boolean persistenceEnabled, BufferConfig cfg) {
    this.level = level;
    this.name = name;
    this.cfg = cfg;

    log.info("-> buffer:'" + cfg.buffer + "'");

    Configuration config = new ConfigurationImpl();
    config.setName(name);
    config.setSecurityEnabled(false);
    config.setPersistenceEnabled(persistenceEnabled);
    config.setJournalDirectory(cfg.buffer + "/journal");
    config.setBindingsDirectory(cfg.buffer + "/bindings");
    config.setLargeMessagesDirectory(cfg.buffer + "/largemessages");
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

  @TestOnly
  public void setQueueSize(QueueInfo key, long queueSize) {
    AddressSettings addressSetting =
        new AddressSettings()
            .setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL)
            .setMaxSizeMessages(-1)
            .setMaxSizeBytes(queueSize);
    amq.getActiveMQServer().getAddressSettingsRepository().addMatch(key.getQueue(), addressSetting);
  }

  @Override
  public void registerNewQueueInfo(QueueInfo key) {
    AddressSettings addressSetting =
        new AddressSettings()
            .setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL)
            .setMaxExpiryDelay(-1L)
            .setMaxDeliveryAttempts(-1);
    amq.getActiveMQServer().getAddressSettingsRepository().addMatch(key.getQueue(), addressSetting);

    createQueue(key.getQueue());

    try {
      registerQueueMetrics(key);
    } catch (MalformedObjectNameException e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }
  }

  @Override
  public void createBridge(String target, QueueInfo key, int targetLevel) {
    String queue = key.getQueue();
    createQueue(queue + ".dl");

    AddressSettings addressSetting_dl =
        new AddressSettings().setMaxExpiryDelay(-1L).setMaxDeliveryAttempts(-1);
    amq.getActiveMQServer().getAddressSettingsRepository().addMatch(queue, addressSetting_dl);

    AddressSettings addressSetting =
        new AddressSettings()
            .setMaxExpiryDelay(cfg.msgExpirationTime)
            .setMaxDeliveryAttempts(cfg.msgRetry)
            .setAddressFullMessagePolicy(AddressFullMessagePolicy.FAIL)
            .setDeadLetterAddress(SimpleString.toSimpleString(queue + ".dl::" + queue + ".dl"))
            .setExpiryAddress(SimpleString.toSimpleString(queue + ".dl::" + queue + ".dl"));
    amq.getActiveMQServer().getAddressSettingsRepository().addMatch(queue, addressSetting);

    BridgeConfiguration bridge =
        new BridgeConfiguration()
            .setName(queue + "." + name + ".to." + target)
            .setQueueName(queue + ".dl::" + queue + ".dl")
            .setForwardingAddress(queue + "::" + queue)
            .setStaticConnectors(Collections.singletonList("to." + target));

    try {
      amq.getActiveMQServer()
          .getConfiguration()
          .addConnectorConfiguration("to." + target, "vm://" + targetLevel);
      amq.getActiveMQServer().deployBridge(bridge);
    } catch (Exception e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }
  }

  void registerQueueMetrics(QueueInfo key) throws MalformedObjectNameException {
    ObjectName queueObjectName =
        new ObjectName(
            String.format(
                "org.apache.activemq.artemis:broker=\"%s\",component=addresses,address=\"%s\",subcomponent=queues,routing-type=\"anycast\",queue=\"%s\"",
                name, key.getQueue(), key.getQueue()));
    ObjectName addressObjectName =
        new ObjectName(
            String.format(
                "org.apache.activemq.artemis:broker=\"%s\",component=addresses,address=\"%s\"",
                name, key.getQueue()));
    Gauge<Long> mc =
        Metrics.newGauge(
            new MetricName("buffer." + name + "." + key.getQueue(), "", "MessageCount"),
            new Gauge<Long>() {
              @Override
              public Long value() {
                Long mc;
                try {
                  mc = (Long) mbServer.getAttribute(queueObjectName, "MessageCount");
                } catch (Exception e) {
                  e.printStackTrace();
                  return 0L;
                }
                return mc;
              }
            });
    mcMetrics.put(key.getQueue(), mc);
    Metrics.newGauge(
        new MetricName("buffer." + name + "." + key.getQueue(), "", "usage"),
        new Gauge<Integer>() {
          @Override
          public Integer value() {
            Integer mc;
            try {
              mc = (Integer) mbServer.getAttribute(addressObjectName, "AddressLimitPercent");
            } catch (Exception e) {
              e.printStackTrace();
              return 0;
            }
            return mc;
          }
        });

    Histogram ms =
        Metrics.newHistogram(
            new MetricName("buffer." + name + "." + key.getQueue(), "", "MessageSize"));
    msMetrics.put(key.getQueue(), ms);
  }

  @Override
  public void sendMsg(QueueInfo key, List<String> strPoints) throws ActiveMQAddressFullException {
    String sessionKey = "sendMsg." + key.getQueue() + "." + Thread.currentThread().getName();
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
                    session.createProducer(key.getQueue() + "::" + key.getQueue());
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
        msMetrics.get(key.getQueue()).update(message.getWholeMessageSize());
        producer.send(message);
      }
      session.commit();
    } catch (ActiveMQAddressFullException e) {
      log.log(Level.FINE, "queue full: " + e.getMessage());
      throw e;
    } catch (Exception e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }
  }

  @Override
  @VisibleForTesting
  public Gauge<Long> getMcGauge(QueueInfo QueueInfo) {
    return mcMetrics.get(QueueInfo.getQueue());
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
  public void onMsgBatch(
      QueueInfo key, int batchSize, RecyclableRateLimiter rateLimiter, OnMsgFunction func) {
    String sessionKey = "onMsgBatch." + key.getQueue() + "." + Thread.currentThread().getName();
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
                    session.createConsumer(key.getQueue() + "::" + key.getQueue());
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
        ClientMessage msg = consumer.receive(100);
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
        log.log(Level.SEVERE, e.getMessage());
        if (log.isLoggable(Level.FINER)) {
          log.log(Level.SEVERE, "error", e);
        }
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

  private void createQueue(String queueName) {
    try {
      QueueConfiguration queue =
          new QueueConfiguration(queueName)
              .setAddress(queueName)
              .setRoutingType(RoutingType.ANYCAST);

      ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://" + level);
      ClientSessionFactory factory = serverLocator.createSessionFactory();
      ClientSession session = factory.createSession();
      ClientSession.QueueQuery q = session.queueQuery(queue.getName());
      if (!q.isExists()) {
        session.createQueue(queue);
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }
  }
}
