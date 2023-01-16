package com.wavefront.agent.core.buffers;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.util.JmxGauge;
import java.util.List;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskBuffer extends ActiveMQBuffer implements Buffer {
  private static final Logger log = LoggerFactory.getLogger(DiskBuffer.class.getCanonicalName());
  private static final Logger slowLog = log;
  //      new MessageDedupingLogger(LoggerFactory.getLogger(MemoryBuffer.class.getCanonicalName()),
  // 1000, 1);

  public DiskBuffer(int level, String name, DiskBufferConfig cfg) {
    super(level, name, true, cfg.buffer, cfg.maxMemory);
    this.compress = true;

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

  // @Override
  // protected String getUrl() {
  // return "tcp://localhost:61616";
  // }

  @Override
  public void sendPoints(String queue, List<String> points) throws ActiveMQAddressFullException {
    if (isFull()) {
      slowLog.error("Memory Queue full");
      throw new ActiveMQAddressFullException();
    }
    super.sendPoints(queue, points);
  }

  @Override
  public String getName() {
    return "Disk";
  }

  @Override
  public int getPriority() {
    return Thread.NORM_PRIORITY;
  }

  public boolean isFull() {
    return activeMQServer.getPagingManager().isDiskFull();
  }

  public void truncate() {
    Object[] addresses = activeMQServer.getManagementService().getResources(AddressControl.class);

    try {
      for (Object obj : addresses) {
        AddressControl address = (AddressControl) obj;
        if (!address.getAddress().startsWith("active")) {
          address.purge();
          log.info(address.getAddress() + " buffer truncated");
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
