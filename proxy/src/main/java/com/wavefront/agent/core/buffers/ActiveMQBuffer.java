package com.wavefront.agent.core.buffers;

import static org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy.FAIL;
import static org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy.PAGE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.common.Pair;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.util.JmxGauge;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.jetbrains.annotations.TestOnly;

public abstract class ActiveMQBuffer implements Buffer, BufferBatch {
  private static final Logger log = Logger.getLogger(BuffersManager.class.getCanonicalName());

  private final EmbeddedActiveMQ amq;

  private final Map<String, Pair<ClientSession, ClientProducer>> producers =
      new ConcurrentHashMap<>();
  private final Map<String, Pair<ClientSession, ClientConsumer>> consumers =
      new ConcurrentHashMap<>();

  private final Map<String, Gauge<Object>> sizeMetrics = new HashMap<>();
  private final Map<String, Histogram> msMetrics = new HashMap<>();
  private final String name;
  @org.jetbrains.annotations.NotNull private final BufferConfig cfg;
  private final int level;
  private final MBeanServer mbServer;
  AtomicInteger qIdxs2 = new AtomicInteger(0);
  private boolean persistenceEnabled;

  public ActiveMQBuffer(int level, String name, boolean persistenceEnabled, BufferConfig cfg) {
    this.level = level;
    this.name = name;
    this.persistenceEnabled = persistenceEnabled;
    this.cfg = cfg;

    log.info("-> buffer:'" + cfg.buffer + "'");

    Configuration config = new ConfigurationImpl();
    config.setName(name);
    config.setSecurityEnabled(false);
    config.setPersistenceEnabled(persistenceEnabled);
    config.setJournalDirectory(cfg.buffer + "/journal");
    config.setBindingsDirectory(cfg.buffer + "/bindings");
    config.setLargeMessagesDirectory(cfg.buffer + "/largemessages");
    config.setPagingDirectory(cfg.buffer + "/paging");
    config.setCreateBindingsDir(true);
    config.setCreateJournalDir(true);
    config.setMessageExpiryScanPeriod(persistenceEnabled ? 0 : 1_000);

    if (persistenceEnabled) {
      config.setGlobalMaxSize((long) ((Runtime.getRuntime().maxMemory() / 2f) / 0.25));
    } else {
      config.setGlobalMaxSize((long) ((Runtime.getRuntime().maxMemory() / 2f) / 0.75));
    }

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
  public void setQueueSize(com.wavefront.agent.core.queues.QueueInfo key, long queueSize) {
    AddressSettings addressSetting =
        new AddressSettings()
            .setAddressFullMessagePolicy(FAIL)
            .setMaxSizeMessages(-1)
            .setMaxSizeBytes(queueSize);
    amq.getActiveMQServer().getAddressSettingsRepository().addMatch(key.getName(), addressSetting);
  }

  @Override
  public void registerNewQueueInfo(com.wavefront.agent.core.queues.QueueInfo key) {
    AddressSettings addressSetting =
        new AddressSettings()
            .setAddressFullMessagePolicy(persistenceEnabled ? PAGE : FAIL)
            .setMaxSizeMessages(-1)
            .setMaxExpiryDelay(-1L)
            .setMaxDeliveryAttempts(-1);
    if (persistenceEnabled) {
      addressSetting.setMaxSizeBytes(-1);
    }
    amq.getActiveMQServer().getAddressSettingsRepository().addMatch(key.getName(), addressSetting);

    // TODO this should be "FlushThreads" and it have to be on sync with SenderTaskFactoryImpl
    for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
      createQueue(key.getName(), i);
    }

    try {
      registerQueueMetrics(key);
    } catch (MalformedObjectNameException e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }
  }

  @Override
  public void createBridge(
      String target, com.wavefront.agent.core.queues.QueueInfo key, int targetLevel) {
    String queue = key.getName();
    createQueue(queue + ".dl", -1);

    AddressSettings addressSetting =
        new AddressSettings()
            .setMaxExpiryDelay(cfg.msgExpirationTime)
            .setMaxDeliveryAttempts(cfg.msgRetry)
            .setAddressFullMessagePolicy(FAIL)
            .setDeadLetterAddress(SimpleString.toSimpleString(queue + ".dl"))
            .setExpiryAddress(SimpleString.toSimpleString(queue + ".dl"));
    amq.getActiveMQServer().getAddressSettingsRepository().addMatch(queue, addressSetting);

    BridgeConfiguration bridge =
        new BridgeConfiguration()
            .setName(queue + "." + name + ".to." + target)
            .setQueueName(queue + ".dl")
            .setForwardingAddress(queue)
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

  void registerQueueMetrics(com.wavefront.agent.core.queues.QueueInfo key)
      throws MalformedObjectNameException {
    //    ObjectName queueObjectName =
    //        new ObjectName(
    //            String.format(
    //
    // "org.apache.activemq.artemis:broker=\"%s\",component=addresses,address=\"%s\",subcomponent=queues,routing-type=\"anycast\",queue=\"%s\"",
    //                name, key.getName(), key.getName()));
    ObjectName addressObjectName =
        new ObjectName(
            String.format(
                "org.apache.activemq.artemis:broker=\"%s\",component=addresses,address=\"%s\"",
                name, key.getName()));

    Gauge<Object> size =
        Metrics.newGauge(
            new MetricName("buffer." + name + "." + key.getName(), "", "size"),
            new JmxGauge(addressObjectName, "AddressSize"));
    sizeMetrics.put(key.getName(), size);

    Metrics.newGauge(
        new MetricName("buffer." + name + "." + key.getName(), "", "usage"),
        new JmxGauge(addressObjectName, "AddressLimitPercent"));

    //    Metrics.newGauge(
    //        new TaggedMetricName(key.getName(), "queued", "reason", "expired"),
    //        new JmxGauge(addressObjectName, "MessagesExpired"));
    //
    //    Metrics.newGauge(
    //        new TaggedMetricName(key.getName(), "queued", "reason", "failed"),
    //        new JmxGauge(addressObjectName, "MessagesKilled"));

    Histogram ms =
        Metrics.newHistogram(
            new MetricName("buffer." + name + "." + key.getName(), "", "MessageSize"));
    msMetrics.put(key.getName(), ms);
  }

  @Override
  public void sendMsgs(com.wavefront.agent.core.queues.QueueInfo key, List<String> points)
      throws ActiveMQAddressFullException {
    String sessionKey = "sendMsg." + key.getName() + "." + Thread.currentThread().getName();
    Pair<ClientSession, ClientProducer> mqCtx =
        producers.computeIfAbsent(
            sessionKey,
            s -> {
              try {
                ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://" + level);
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                ClientSession session = factory.createSession(true, true);
                ClientProducer producer = session.createProducer(key.getName());
                return new Pair<>(session, producer);
              } catch (Exception e) {
                e.printStackTrace();
              }
              return null;
            });

    // TODO: check if session still valid
    ClientSession session = mqCtx._1;
    ClientProducer producer = mqCtx._2;
    try {
      ClientMessage message = session.createMessage(true);
      message.writeBodyBufferString(String.join("\n", points));
      // TODO: reimplement Merict size
      //      msMetrics.get(key.getName()).update(message.getWholeMessageSize());
      producer.send(message);
    } catch (ActiveMQAddressFullException e) {
      log.log(Level.FINE, "queue full: " + e.getMessage());
      throw e;
    } catch (ActiveMQObjectClosedException e) {
      log.log(Level.FINE, "connection close: " + e.getMessage());
      producers.remove(key.getName());
    } catch (Exception e) {
      log.log(Level.SEVERE, "error", e);
    }
  }

  @VisibleForTesting
  protected Gauge<Object> getSizeGauge(com.wavefront.agent.core.queues.QueueInfo q) {
    return sizeMetrics.get(q.getName());
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
      com.wavefront.agent.core.queues.QueueInfo key,
      int batchSize,
      RecyclableRateLimiter rateLimiter,
      OnMsgFunction func) {
    String sessionKey = "onMsgBatch." + key.getName() + "." + Thread.currentThread().getName();
    Pair<ClientSession, ClientConsumer> mqCtx =
        consumers.computeIfAbsent(
            sessionKey,
            s -> {
              try {
                ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://" + level);
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                // 2sd false means that we send msg.ack only on session.commit
                ClientSession session = factory.createSession(false, false);

                int idx = qIdxs2.getAndIncrement();
                createQueue(key.getName(), idx);

                ClientConsumer consumer =
                    session.createConsumer(key.getName() + "::" + key.getName() + "." + idx);
                return new Pair<>(session, consumer);
              } catch (Exception e) {
                e.printStackTrace();
              }
              return null;
            });

    ClientSession session = mqCtx._1;
    ClientConsumer consumer = mqCtx._2;
    try {
      long start = System.currentTimeMillis();
      session.start();
      List<String> batch = new ArrayList<>(batchSize);
      boolean done = false;
      while ((batch.size() < batchSize) && !done && ((System.currentTimeMillis() - start) < 1000)) {
        ClientMessage msg = consumer.receive(100);
        if (msg != null) {
          List msgs = Arrays.asList(msg.getReadOnlyBodyBuffer().readString().split("\n"));
          boolean ok = rateLimiter.tryAcquire(msgs.size());
          if (ok) {
            msg.acknowledge();
            batch.addAll(msgs);
          } else {
            log.info("rate limit reached on queue '" + key.getName() + "'");
            done = true;
          }
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
    } catch (Throwable e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    } finally {
      try {
        session.stop();
      } catch (ActiveMQException e) {
        log.log(Level.SEVERE, "error", e);
      }
    }
  }

  private void createQueue(String queueName, int i) {
    try {
      QueueConfiguration queue =
          new QueueConfiguration(queueName + (i < 0 ? "" : ("." + i)))
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
