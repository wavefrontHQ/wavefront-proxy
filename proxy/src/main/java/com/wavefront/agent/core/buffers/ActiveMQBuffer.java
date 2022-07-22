package com.wavefront.agent.core.buffers;

import static org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy.FAIL;
import static org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy.PAGE;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.common.Pair;
import com.wavefront.common.logger.MessageDedupingLogger;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.util.JmxGauge;
import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;

@SuppressWarnings("ALL")
public abstract class ActiveMQBuffer implements Buffer {
  private static final Logger log = Logger.getLogger(ActiveMQBuffer.class.getCanonicalName());
  private static final Logger slowLog =
      new MessageDedupingLogger(Logger.getLogger(ActiveMQBuffer.class.getCanonicalName()), 1000, 1);

  final EmbeddedActiveMQ amq;

  private final Map<String, Pair<ClientSession, ClientProducer>> producers =
      new ConcurrentHashMap<>();
  private final Map<String, Pair<ClientSession, ClientConsumer>> consumers =
      new ConcurrentHashMap<>();

  protected final Map<String, PointsGauge> countMetrics = new HashMap<>();
  private final Map<String, Gauge<Object>> sizeMetrics = new HashMap<>();
  private final Map<String, Histogram> timeMetrics = new HashMap<>();

  final String name;
  private final int serverID;
  protected Buffer nextBuffer;

  public ActiveMQBuffer(
      int serverID, String name, boolean persistenceEnabled, File buffer, long maxMemory) {
    this.serverID = serverID;
    this.name = name;

    Configuration config = new ConfigurationImpl();
    config.setName(name);
    config.setSecurityEnabled(false);
    config.setPersistenceEnabled(persistenceEnabled);
    config.setMessageExpiryScanPeriod(persistenceEnabled ? 0 : 1_000);
    config.setGlobalMaxSize(maxMemory);

    if (persistenceEnabled) {
      config.setMaxDiskUsage(70);
      config.setJournalDirectory(new File(buffer, "journal").getAbsolutePath());
      config.setBindingsDirectory(new File(buffer, "bindings").getAbsolutePath());
      config.setLargeMessagesDirectory(new File(buffer, "largemessages").getAbsolutePath());
      config.setPagingDirectory(new File(buffer, "paging").getAbsolutePath());
      config.setCreateBindingsDir(true);
      config.setCreateJournalDir(true);
    }

    amq = new EmbeddedActiveMQ();

    try {
      config.addAcceptorConfiguration("in-vm", "vm://" + serverID);
      amq.setConfiguration(config);
      amq.start();
    } catch (Exception e) {
      log.log(Level.SEVERE, "error creating buffer", e);
      System.exit(-1);
    }

    AddressSettings addressSetting =
        new AddressSettings()
            .setMaxSizeMessages(-1)
            .setMaxExpiryDelay(-1L)
            .setMaxDeliveryAttempts(-1)
            .setManagementBrowsePageSize(Integer.MAX_VALUE);

    if (persistenceEnabled) {
      addressSetting.setMaxSizeBytes(-1);
      addressSetting.setAddressFullMessagePolicy(PAGE);
    } else {
      addressSetting.setMaxSizeBytes(maxMemory);
      addressSetting.setAddressFullMessagePolicy(FAIL);
    }

    amq.getActiveMQServer().getAddressSettingsRepository().setDefault(addressSetting);
  }

  @Override
  public void registerNewQueueInfo(QueueInfo queue) {
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

    countMetrics.put(
        queue.getName(),
        (PointsGauge)
            Metrics.newGauge(
                new MetricName("buffer." + name + "." + queue.getName(), "", "points"),
                new PointsGauge(queue, amq)));

    timeMetrics.put(
        queue.getName(),
        Metrics.newHistogram(
            new MetricName("buffer." + name + "." + queue.getName(), "", "queue-time")));
  }

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
  public void sendPoints(String queue, List<String> points) throws ActiveMQAddressFullException {
    try {
      doSendPoints(queue, points);
    } catch (ActiveMQAddressFullException e) {
      slowLog.log(Level.SEVERE, "Memory Queue full");
      if (slowLog.isLoggable(Level.FINER)) {
        slowLog.log(Level.SEVERE, "", e);
      }
      if (nextBuffer != null) {
        nextBuffer.sendPoints(queue, points);
        QueueStats.get(queue).queuedFull.inc();
      } else {
        throw e;
      }
    }
  }

  public void doSendPoints(String queue, List<String> points) throws ActiveMQAddressFullException {
    String sessionKey = "sendMsg." + queue + "." + Thread.currentThread().getName();
    Pair<ClientSession, ClientProducer> mqCtx =
        producers.computeIfAbsent(
            sessionKey,
            s -> {
              try {
                ServerLocator serverLocator =
                    ActiveMQClient.createServerLocator("vm://" + serverID);
                ClientSessionFactory factory = serverLocator.createSessionFactory();
                ClientSession session = factory.createSession();
                ClientProducer producer = session.createProducer(queue);
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
      producers.remove(queue);
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
      boolean done = false;
      boolean needRollBack = false;
      while ((batch.size() < batchSize) && !done && ((System.currentTimeMillis() - start) < 1000)) {
        ClientMessage msg = consumer.receive(100);
        if (msg != null) {
          List<String> points = Arrays.asList(msg.getReadOnlyBodyBuffer().readString().split("\n"));
          boolean ok = rateLimiter.tryAcquire(points.size());
          if (ok) {
            toACK.add(msg);
            batch.addAll(points);
          } else {
            slowLog.info("rate limit reached on queue '" + queue.getName() + "'");
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
                timeMetrics.get(queue.getName()).update(start - msg.getTimestamp());
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

  public void setNextBuffer(Buffer nextBuffer) {
    this.nextBuffer = nextBuffer;
  }
}
