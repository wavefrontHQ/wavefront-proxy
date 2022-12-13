package com.wavefront.agent.core.buffers;

import static org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy.FAIL;
import static org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy.PAGE;

import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.util.JmxGauge;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ActiveMQBuffer implements Buffer {
  private static final Logger log =
      LoggerFactory.getLogger(ActiveMQBuffer.class.getCanonicalName());
  private static final Logger slowLog = log;
  public static final String MSG_ITEMS = "items";
  public static final String MSG_BYTES = "bytes";
  // new
  // MessageDedupingLogger(LoggerFactory.getLogger(ActiveMQBuffer.class.getCanonicalName()),
  // 1000,
  // 1);
  protected final Map<String, PointsGauge> countMetrics = new HashMap<>();
  final ActiveMQServer activeMQServer;
  final String name;
  private final Map<String, Session> producers = new ConcurrentHashMap<>();
  private final Map<String, Session> consumers = new ConcurrentHashMap<>();
  private final Map<String, Gauge<Object>> sizeMetrics = new HashMap<>();
  private final Map<String, Histogram> timeMetrics = new HashMap<>();
  private final int serverID;
  protected Buffer nextBuffer;
  private ServerLocator serverLocator;
  private ClientSessionFactory factory;

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

    try {
      Path tmpBuffer = Files.createTempDirectory("wfproxy");
      config.setPagingDirectory(tmpBuffer.toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (persistenceEnabled) {
      config.setMaxDiskUsage(70);
      config.setJournalDirectory(new File(buffer, "journal").getAbsolutePath());
      config.setBindingsDirectory(new File(buffer, "bindings").getAbsolutePath());
      config.setLargeMessagesDirectory(new File(buffer, "largemessages").getAbsolutePath());
      config.setPagingDirectory(new File(buffer, "paging").getAbsolutePath());
      config.setCreateBindingsDir(true);
      config.setCreateJournalDir(true);
      config.setJournalLockAcquisitionTimeout(10);
      config.setJournalType(JournalType.NIO);
    }

    ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager();
    activeMQServer = new ActiveMQServerImpl(config, securityManager);
    activeMQServer.registerActivationFailureListener(
        exception ->
            log.error(
                "error creating buffer, "
                    + exception.getMessage()
                    + ". Review if there is another Proxy running."));

    try {
      config.addAcceptorConfiguration("in-vm", "vm://" + serverID);
      activeMQServer.start();
    } catch (Exception e) {
      log.error("error creating buffer", e);
      System.exit(-1);
    }

    if (!activeMQServer.isActive()) {
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

    activeMQServer.getAddressSettingsRepository().setDefault(addressSetting);
  }

  protected String getUrl() {
    return "vm://" + serverID;
  }

  @Override
  public void registerNewQueueInfo(QueueInfo queue) {
    for (int i = 0; i < queue.getNumberThreads(); i++) {
      createQueue(queue.getName(), i);
    }

    try {
      registerQueueMetrics(queue);
    } catch (MalformedObjectNameException e) {
      log.error("error", e);
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
                new PointsGauge(queue, activeMQServer)));

    timeMetrics.put(
        queue.getName(),
        Metrics.newHistogram(
            new MetricName("buffer." + name + "." + queue.getName(), "", "queue-time")));
  }

  public void shutdown() {
    try {
      for (Map.Entry<String, Session> entry : producers.entrySet()) {
        entry.getValue().close(); // session
        entry.getValue().close(); // producer
      }
      for (Map.Entry<String, Session> entry : consumers.entrySet()) {
        entry.getValue().close(); // session
        entry.getValue().close(); // consumer
      }

      activeMQServer.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void sendPoints(String queue, List<String> points) throws ActiveMQAddressFullException {
    try {
      doSendPoints(queue, points);
    } catch (ActiveMQAddressFullException e) {
      slowLog.error("Memory Queue full");
      if (slowLog.isDebugEnabled()) {
        slowLog.error("", e);
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
    Session mqCtx =
        producers.computeIfAbsent(
            sessionKey,
            s -> {
              try {
                checkConnection();
                ClientSession session = factory.createSession();
                ClientProducer producer = session.createProducer(queue);
                return new Session(session, producer);
              } catch (Exception e) {
                checkException(e);
                throw new RuntimeException(e);
              }
            });

    try {
      ClientMessage message = mqCtx.session.createMessage(true);
      String str = String.join("\n", points);
      message.writeBodyBufferString(str);
      message.putIntProperty(MSG_ITEMS, points.size());
      message.putIntProperty(MSG_BYTES, str.length());
      mqCtx.producer.send(message);
    } catch (ActiveMQAddressFullException e) {
      log.info("queue full: " + e.getMessage());
      throw e;
    } catch (ActiveMQObjectClosedException e) {
      log.info("connection close: " + e.getMessage());
      mqCtx.close();
      producers.remove(sessionKey);
      QueueStats.get(queue).internalError.inc();
      if (nextBuffer != null) {
        nextBuffer.sendPoints(queue, points);
      } else {
        sendPoints(queue, points);
      }
    } catch (Exception e) {
      log.error("error", e);
      throw new RuntimeException(e);
    }
  }

  private void checkConnection() throws Exception {
    if ((serverLocator == null) || (serverLocator.isClosed())) {
      serverLocator = ActiveMQClient.createServerLocator(getUrl());
    }
    if ((factory == null) || (factory.isClosed())) {
      factory = serverLocator.createSessionFactory();
    }
  }

  @Override
  public void onMsgBatch(QueueInfo queue, int idx, OnMsgDelegate delegate) {
    String sessionKey = "onMsgBatch." + queue.getName() + "." + Thread.currentThread().getName();
    Session mqCtx =
        consumers.computeIfAbsent(
            sessionKey,
            s -> {
              try {
                checkConnection();
                ClientSession session = factory.createSession(false, false);
                ClientConsumer consumer = session.createConsumer(queue.getName() + "." + idx);
                return new Session(session, consumer);
              } catch (Exception e) {
                checkException(e);
                if (e instanceof ActiveMQConnectionTimedOutException) {
                  createQueue(queue.getName(), idx);
                }
                throw new RuntimeException(e);
              }
            });

    try {
      long start = System.currentTimeMillis();
      mqCtx.session.start();
      List<String> batch = new ArrayList<>();
      List<ClientMessage> toACK = new ArrayList<>();
      boolean done = false;
      boolean needRollBack = false;
      int batchBytes = 0;
      while (!done && ((System.currentTimeMillis() - start) < 1000)) {
        ClientMessage msg = mqCtx.consumer.receive(100);
        if (msg != null) {
          List<String> points;
          ByteBuf buffer = msg.getBuffer();
          if (buffer != null) {
            points = Arrays.asList(msg.getReadOnlyBodyBuffer().readString().split("\n"));
          } else {
            points = new ArrayList<>();
            log.warn("Empty message");
          }
          boolean ok_size =
              delegate.checkBatchSize(
                  batch.size(), batchBytes, points.size(), msg.getIntProperty(MSG_BYTES));
          boolean ok_rate = delegate.checkRates(points.size(), batchBytes);
          if (ok_size && ok_rate) {
            toACK.add(msg);
            batch.addAll(points);
            batchBytes += msg.getIntProperty(MSG_BYTES);
          } else {
            if (!ok_rate) {
              slowLog.info("rate limit reached on queue '" + queue.getName() + "'");
            } else {
              slowLog.info("payload limit reached on queue '" + queue.getName() + "'");
            }
            done = true;
            needRollBack = true;
          }
        } else {
          done = true;
        }
      }

      try {
        if (batch.size() > 0) {
          delegate.processBatch(batch);
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
        mqCtx.session.commit();
        if (needRollBack) {
          // rollback all messages not ACKed (rate)
          mqCtx.session.rollback();
        }
      } catch (Exception e) {
        log.error(e.toString());
        if (log.isDebugEnabled()) {
          log.error("error", e);
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
        mqCtx.session.rollback();
      }
    } catch (Throwable e) {
      log.error("error", e);
      mqCtx.close();
      consumers.remove(sessionKey);
    } finally {
      try {
        if (!mqCtx.session.isClosed()) {
          mqCtx.session.stop();
        }
      } catch (ActiveMQException e) {
        log.error("error", e);
        mqCtx.close();
        consumers.remove(sessionKey);
      }
    }
  }

  private void checkException(Exception e) {
    if (e instanceof ActiveMQNotConnectedException) {
      serverLocator = null;
      factory = null;
    }
  }

  private void createQueue(String queueName, int i) {
    QueueConfiguration queue =
        new QueueConfiguration(queueName + (i < 0 ? "" : ("." + i)))
            .setAddress(queueName)
            .setRoutingType(RoutingType.ANYCAST);

    try (ServerLocator sl = ActiveMQClient.createServerLocator(getUrl());
        ClientSessionFactory f = sl.createSessionFactory();
        ClientSession session = f.createSession()) {
      ClientSession.QueueQuery q = session.queueQuery(queue.getName());
      if (!q.isExists()) {
        session.createQueue(queue);
      }
    } catch (Exception e) {
      log.error("error", e);
    }
  }

  public void setNextBuffer(Buffer nextBuffer) {
    this.nextBuffer = nextBuffer;
  }

  private class Session {
    ClientSession session;
    ClientConsumer consumer;
    ClientProducer producer;

    Session(ClientSession session, ClientConsumer consumer) {
      this.session = session;
      this.consumer = consumer;
    }

    public Session(ClientSession session, ClientProducer producer) {
      this.session = session;
      this.producer = producer;
    }

    void close() {
      if (session != null) {
        try {
          session.close();
        } catch (Throwable e) {
        }
      }
      if (consumer != null) {
        try {
          consumer.close();
        } catch (Throwable e) {
        }
      }
      if (producer != null) {
        try {
          producer.close();
        } catch (Throwable e) {
        }
      }
    }
  }
}
