package com.wavefront.agent.core.buffers;

import static org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy.FAIL;
import static org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy.PAGE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.core.queues.QueueInfo;
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
  private static final Logger log = Logger.getLogger(ActiveMQBuffer.class.getCanonicalName());

  final EmbeddedActiveMQ amq;

  private final Map<String, Pair<ClientSession, ClientProducer>> producers =
      new ConcurrentHashMap<>();
  private final Map<String, Pair<ClientSession, ClientConsumer>> consumers =
      new ConcurrentHashMap<>();

  private final Map<String, Gauge<Long>> countMetrics = new HashMap<>();
  private final Map<String, Gauge<Object>> sizeMetrics = new HashMap<>();
  private final Map<String, Histogram> msMetrics = new HashMap<>();

  final String name;
  @org.jetbrains.annotations.NotNull private final BufferConfig cfg;
  private final int serverID;
  private final MBeanServer mbServer;
  AtomicInteger qIdxs2 = new AtomicInteger(0);
  private boolean persistenceEnabled;

  public ActiveMQBuffer(int serverID, String name, boolean persistenceEnabled, BufferConfig cfg) {
    this.serverID = serverID;
    this.name = name;
    this.persistenceEnabled = persistenceEnabled;
    this.cfg = cfg;

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

    // TODO: make it configurable
    if (persistenceEnabled) {
      config.setGlobalMaxSize(256_000_000);
    } else {
      config.setGlobalMaxSize(768_000_000);
    }

    amq = new EmbeddedActiveMQ();

    try {
      TransportConfiguration trans = new TransportConfiguration();
      config.addAcceptorConfiguration("in-vm", "vm://" + serverID);
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
            .setAddressFullMessagePolicy(FAIL)
            .setMaxSizeMessages(-1)
            .setMaxSizeBytes(queueSize);
    amq.getActiveMQServer().getAddressSettingsRepository().addMatch(key.getName(), addressSetting);
  }

  @Override
  public void registerNewQueueInfo(QueueInfo queue) {
    AddressSettings addressSetting =
        new AddressSettings()
            .setAddressFullMessagePolicy(persistenceEnabled ? PAGE : FAIL)
            .setMaxSizeMessages(-1)
            .setMaxExpiryDelay(-1L)
            .setMaxDeliveryAttempts(-1);
    if (persistenceEnabled) {
      addressSetting.setMaxSizeBytes(-1);
    }
    amq.getActiveMQServer()
        .getAddressSettingsRepository()
        .addMatch(queue.getName(), addressSetting);

    for (int i = 0; i < queue.getNumberThreads(); i++) {
      createQueue(queue.getName(), i);
    }

    try {
      registerQueueMetrics(queue);
    } catch (MalformedObjectNameException e) {
      log.log(Level.SEVERE, "error", e);
      System.exit(-1);
    }
  }

  @Override
  public void createBridge(String target, QueueInfo key, int targetLevel) {
    String queue = key.getName();
    String queue_dl = queue + ".dl";
    createQueue(queue_dl, -1);

    AddressSettings addressSetting =
        new AddressSettings()
            .setMaxExpiryDelay(cfg.msgExpirationTime)
            .setMaxDeliveryAttempts(cfg.msgRetry)
            .setAddressFullMessagePolicy(FAIL)
            .setDeadLetterAddress(SimpleString.toSimpleString(queue_dl))
            .setExpiryAddress(SimpleString.toSimpleString(queue_dl));
    amq.getActiveMQServer().getAddressSettingsRepository().addMatch(queue, addressSetting);

    BridgeConfiguration bridge =
        new BridgeConfiguration()
            .setName(name + "." + queue + ".to." + target)
            .setQueueName(queue_dl)
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

  void registerQueueMetrics(QueueInfo queue) throws MalformedObjectNameException {
    ObjectName addressObjectName =
        new ObjectName(
            String.format(
                "org.apache.activemq.artemis:broker=\"%s\",component=addresses,address=\"%s\"",
                name, queue.getName()));

    sizeMetrics.put(
        queue.getName(),
        Metrics.newGauge(
            new MetricName("buffer." + name + "." + queue.getName(), "", "size"),
            new JmxGauge(addressObjectName, "AddressSize")));

    Metrics.newGauge(
        new MetricName("buffer." + name + "." + queue.getName(), "", "usage"),
        new JmxGauge(addressObjectName, "AddressLimitPercent"));

    // TODO
    //    Histogram ms =
    //        Metrics.newHistogram(
    //            new MetricName("buffer." + name + "." + queue.getName(), "", "MessageSize"));
    //    msMetrics.put(queue.getName(), ms);

    countMetrics.put(
        queue.getName(),
        Metrics.newGauge(
            new MetricName("buffer." + name + "." + queue.getName(), "", "count"),
            new PointsGauge(queue, serverID)));
  }

  @VisibleForTesting
  protected Gauge<Object> getSizeGauge(QueueInfo q) {
    return sizeMetrics.get(q.getName());
  }

  //  protected Gauge<Long> getCountGauge(QueueInfo q) {
  //    return countMetrics.get(q.getName());
  //  }

  @Override
  public void shutdown() {
    try {
      for (Map.Entry<String, Pair<ClientSession, ClientProducer>> entry : producers.entrySet()) {
        entry.getValue()._1.close(); // session
        entry.getValue()._2.close(); // producer
      }
      for (Map.Entry<String, Pair<ClientSession, ClientConsumer>> entry : consumers.entrySet()) {
        entry.getValue()._1.close(); // session
        entry.getValue()._2.close(); // consumer
      }

      amq.stop();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  @Override
  public void sendMsgs(QueueInfo queue, List<String> points) throws ActiveMQAddressFullException {
    String sessionKey = "sendMsg." + queue.getName() + "." + Thread.currentThread().getName();
    Pair<ClientSession, ClientProducer> mqCtx =
        producers.computeIfAbsent(
            sessionKey,
            s -> {
              try {
                ServerLocator serverLocator =
                    ActiveMQClient.createServerLocator("vm://" + serverID);
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                ClientSession session = factory.createSession();
                ClientProducer producer = session.createProducer(queue.getName());
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
      message.putIntProperty("points", points.size());
      // TODO: reimplement Merict size
      //      msMetrics.get(queue.getName()).update(message.getWholeMessageSize());
      producer.send(message);
    } catch (ActiveMQAddressFullException e) {
      log.log(Level.FINE, "queue full: " + e.getMessage());
      throw e;
    } catch (ActiveMQObjectClosedException e) {
      log.log(Level.FINE, "connection close: " + e.getMessage());
      producers.remove(queue.getName());
    } catch (Exception e) {
      log.log(Level.SEVERE, "error", e);
    }
  }

  @Override
  public void onMsgBatch(
      QueueInfo queue,
      int idx,
      int batchSize,
      RecyclableRateLimiter rateLimiter,
      OnMsgFunction func) {
    String sessionKey = "onMsgBatch." + queue.getName() + "." + Thread.currentThread().getName();
    Pair<ClientSession, ClientConsumer> mqCtx =
        consumers.computeIfAbsent(
            sessionKey,
            s -> {
              try {
                ServerLocator serverLocator =
                    ActiveMQClient.createServerLocator("vm://" + serverID);
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                ClientSession session = factory.createSession(false, false);
                ClientConsumer consumer = session.createConsumer(queue.getName() + "." + idx);
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
      List<ClientMessage> toACK = new ArrayList<>();
      List<ClientMessage> allMsgs = new ArrayList<>();
      boolean done = false;
      boolean needRollBack = false;
      while ((batch.size() < batchSize) && !done && ((System.currentTimeMillis() - start) < 1000)) {
        ClientMessage msg = consumer.receive(100);
        if (msg != null) {
          allMsgs.add(msg);
          List msgs = Arrays.asList(msg.getReadOnlyBodyBuffer().readString().split("\n"));
          boolean ok = rateLimiter.tryAcquire(msgs.size());
          if (ok) {
            toACK.add(msg);
            batch.addAll(msgs);
          } else {
            log.info("rate limit reached on queue '" + queue.getName() + "'");
            done = true;
            needRollBack = true;
          }
        } else {
          done = true;
        }
      }

      try {
        if (batch.size() > 0) {
          func.run(batch);
        }
        // commit all messages ACKed
        toACK.forEach(
            msg -> {
              try {
                msg.individualAcknowledge();
              } catch (ActiveMQException e) {
                throw new RuntimeException(e);
              }
            });
        session.commit();
        if (needRollBack) {
          // rollback all messages not ACKed (rate)
          session.rollback();
        }
      } catch (Exception e) {
        log.log(Level.SEVERE, e.getMessage());
        if (log.isLoggable(Level.FINER)) {
          log.log(Level.SEVERE, "error", e);
        }
        // ACK all messages and then rollback so fail count go up
        toACK.forEach(
            msg -> {
              try {
                msg.individualAcknowledge();
              } catch (ActiveMQException ex) {
                throw new RuntimeException(ex);
              }
            });
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

      ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://" + serverID);
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
