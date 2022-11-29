package com.wavefront.agent.core.queues;

import com.wavefront.agent.PushAgent;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.BurstRateTrackingCounter;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class QueueStats {
  public final Counter dropped;
  public final Counter delivered;
  public final Counter failed;
  public final Counter sent;
  public final Counter queuedFailed;
  public final Counter queuedExpired;
  public final Histogram msgLength;
  public final Counter queuedFull;
  public final Counter internalError;

  private final BurstRateTrackingCounter deliveredStats;
  private final QueueInfo queue;

  private static final Map<String, QueueStats> stats = new HashMap<>();
  private static final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(2, new NamedThreadFactory("QueueStats"));

  protected static QueueStats register(QueueInfo queue) {
    return stats.computeIfAbsent(queue.getName(), s -> new QueueStats(queue, executor));
  }

  public static QueueStats get(String queue) {
    return stats.get(queue);
  }

  private QueueStats(QueueInfo queue, ScheduledExecutorService scheduler) {
    this.queue = queue;
    MetricName deliveredMetricName = new MetricName(queue.getName(), "", "delivered");
    this.delivered = Metrics.newCounter(deliveredMetricName);
    this.deliveredStats =
        new BurstRateTrackingCounter(deliveredMetricName, Metrics.defaultRegistry(), 1000);
    this.failed = Metrics.newCounter(new MetricName(queue.getName(), "", "failed"));
    this.sent = Metrics.newCounter(new MetricName(queue.getName(), "", "sent"));
    this.dropped = Metrics.newCounter(new MetricName(queue.getName(), "", "dropped"));

    msgLength =
        Metrics.newHistogram(new MetricName("buffer." + queue.getName(), "", "message_length"));

    queuedFailed =
        Metrics.newCounter(new TaggedMetricName(queue.getName(), "queued", "reason", "failed"));

    queuedExpired =
        Metrics.newCounter(new TaggedMetricName(queue.getName(), "queued", "reason", "expired"));

    queuedFull =
            Metrics.newCounter(new TaggedMetricName(queue.getName(), "queued", "reason", "queue-full"));

    internalError =
            Metrics.newCounter(new TaggedMetricName(queue.getName(), "queued", "reason", "internal-error"));

    scheduler.scheduleAtFixedRate(() -> printStats(), 10, 10, TimeUnit.SECONDS);
    scheduler.scheduleAtFixedRate(() -> printTotal(), 1, 1, TimeUnit.MINUTES);
  }

  protected void printStats() {
    String rateUnit = queue.getEntityType().getRateUnit();
    PushAgent.stats.info(
        "["
            + queue.getName()
            + "] "
            + queue.getEntityType().toCapitalizedString()
            + " delivered rate: "
            + deliveredStats.getOneMinutePrintableRate()
            + " "
            + rateUnit
            + " (1 min), "
            + deliveredStats.getFiveMinutePrintableRate()
            + " "
            + rateUnit
            + " (5 min) "
            + deliveredStats.getCurrentRate()
            + " "
            + rateUnit
            + " (current).");
  }

  protected void printTotal() {
    PushAgent.stats.info(
        "["
            + queue.getName()
            + "] "
            + queue.getEntityType().toCapitalizedString()
            + " sent since start: "
            + this.sent.count()
            + "; delivered: "
            + this.delivered.count()
            + "; failed: "
            + this.failed.count()
            + "; dropped: "
            + this.dropped.count());
  }
}
