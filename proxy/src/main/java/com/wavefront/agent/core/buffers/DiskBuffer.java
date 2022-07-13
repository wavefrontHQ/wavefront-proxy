package com.wavefront.agent.core.buffers;

import com.wavefront.agent.core.queues.QueueInfo;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.util.JmxGauge;
import java.util.logging.Logger;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class DiskBuffer extends ActiveMQBuffer implements Buffer, BufferBatch {
  private static final Logger log = Logger.getLogger(DiskBuffer.class.getCanonicalName());

  public DiskBuffer(int level, String name, BufferConfig cfg) {
    super(level, name, true, cfg);

    try {
      ObjectName addressObjectName =
          new ObjectName(String.format("org.apache.activemq.artemis:broker=\"%s\"", name));
      Metrics.newGauge(
          new MetricName("buffer." + name, "", "diskUsage"),
          new JmxGauge(addressObjectName, "DiskStoreUsage"));
      Metrics.newGauge(
          new MetricName("buffer." + name, "", "diskUsageMax"),
          new JmxGauge(addressObjectName, "MaxDiskUsage"));

    } catch (MalformedObjectNameException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createBridge(String target, QueueInfo queue, int level) {}
}
