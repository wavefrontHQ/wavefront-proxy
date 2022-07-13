package com.wavefront.agent.core.senders;

import com.wavefront.agent.core.queues.QueueInfo;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.BurstRateTrackingCounter;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class SenderStats {
  public Counter delivered;
  protected BurstRateTrackingCounter deliveredStats;
  public Counter failed;
  public Counter sent;
  protected QueueInfo queue;
  private static Logger log = Logger.getLogger(SenderStats.class.getCanonicalName());

  private static final Map<String, SenderStats> stats = new HashMap<>();

  public static SenderStats create(QueueInfo queue, ScheduledExecutorService scheduler) {
    return stats.computeIfAbsent(queue.getName(), s -> new SenderStats(queue, scheduler));
  }

  private SenderStats(QueueInfo queue, ScheduledExecutorService scheduler) {
    this.queue = queue;
    MetricName deliveredMetricName = new MetricName(queue.getName(), "", "delivered");
    this.delivered = Metrics.newCounter(deliveredMetricName);
    this.deliveredStats =
        new BurstRateTrackingCounter(deliveredMetricName, Metrics.defaultRegistry(), 1000);
    this.failed = Metrics.newCounter(new MetricName(queue.getName(), "", "failed"));
    this.sent = Metrics.newCounter(new MetricName(queue.getName(), "", "sent"));

    scheduler.scheduleAtFixedRate(() -> printStats(), 10, 10, TimeUnit.SECONDS);
    scheduler.scheduleAtFixedRate(() -> printTotal(), 1, 1, TimeUnit.MINUTES);
  }

  protected void printStats() {
    String rateUnit = queue.getEntityType().getRateUnit();
    log.info(
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
    log.info(
        "["
            + queue.getName()
            + "] "
            + queue.getEntityType().toCapitalizedString()
            + " sent since start: "
            + this.sent.count()
            + "; delivered: "
            + this.delivered.count()
            + "; failed: "
            + this.failed.count());
  }
}
