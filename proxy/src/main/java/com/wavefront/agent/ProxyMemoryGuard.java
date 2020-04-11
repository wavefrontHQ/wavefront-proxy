package com.wavefront.agent;

import com.google.common.base.Preconditions;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.management.NotificationEmitter;

import static com.wavefront.common.Utils.lazySupplier;

/**
 * Logic around OoM protection logic that drains memory buffers on
 * MEMORY_THRESHOLD_EXCEEDED notifications, extracted from AbstractAgent.
 *
 * @author vasily@wavefront.com
 */
public class ProxyMemoryGuard {
  private static final Logger logger = Logger.getLogger(ProxyMemoryGuard.class.getCanonicalName());

  private final Supplier<Counter> drainBuffersCount = lazySupplier(() ->
      Metrics.newCounter(new TaggedMetricName("buffer", "flush-count",
          "reason", "heapUsageThreshold")));

  /**
   * Set up the memory guard.
   *
   * @param flushTask runnable to invoke when in-memory buffers need to be drained to disk
   * @param threshold memory usage threshold that is considered critical, 0 &lt; threshold &lt;= 1.
   */
  public ProxyMemoryGuard(@Nonnull final Runnable flushTask, double threshold) {
    Preconditions.checkArgument(threshold > 0, "ProxyMemoryGuard threshold must be > 0!");
    Preconditions.checkArgument(threshold <= 1, "ProxyMemoryGuard threshold must be <= 1!");
    MemoryPoolMXBean tenuredGenPool = getTenuredGenPool();
    if (tenuredGenPool == null) return;
    tenuredGenPool.setUsageThreshold((long) (tenuredGenPool.getUsage().getMax() * threshold));

    NotificationEmitter emitter = (NotificationEmitter) ManagementFactory.getMemoryMXBean();
    emitter.addNotificationListener((notification, obj) -> {
      if (notification.getType().equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) {
        logger.warning("Heap usage threshold exceeded - draining buffers to disk!");
        drainBuffersCount.get().inc();
        flushTask.run();
        logger.info("Draining buffers to disk: finished");
      }
    }, null, null);

  }

  private MemoryPoolMXBean getTenuredGenPool() {
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == MemoryType.HEAP && pool.isUsageThresholdSupported()) {
        return pool;
      }
    }
    return null;
  }
}
