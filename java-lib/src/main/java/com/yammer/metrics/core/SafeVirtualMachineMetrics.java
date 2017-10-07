package com.yammer.metrics.core;

import com.sun.management.UnixOperatingSystemMXBean;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.List;

import javax.management.MBeanServer;

/**
 * Java 9 compatible implementation of {@link VirtualMachineMetrics} that doesn't use reflection
 * and is not susceptible to a InaccessibleObjectException in fileDescriptorUsage())
 *
 * @author Vasily Vorontsov (vasily@wavefront.com)
 */
public class SafeVirtualMachineMetrics extends VirtualMachineMetrics {
  private static final VirtualMachineMetrics INSTANCE = new SafeVirtualMachineMetrics(
      ManagementFactory.getMemoryMXBean(), ManagementFactory.getMemoryPoolMXBeans(),
      ManagementFactory.getOperatingSystemMXBean(), ManagementFactory.getThreadMXBean(),
      ManagementFactory.getGarbageCollectorMXBeans(), ManagementFactory.getRuntimeMXBean(),
      ManagementFactory.getPlatformMBeanServer());
  private final OperatingSystemMXBean os;

  /**
   * The default instance of {@link SafeVirtualMachineMetrics}.
   *
   * @return the default {@link SafeVirtualMachineMetrics instance}
   */
  public static VirtualMachineMetrics getInstance() {
    return INSTANCE;
  }

  private SafeVirtualMachineMetrics(MemoryMXBean memory, List<MemoryPoolMXBean> memoryPools, OperatingSystemMXBean os,
                                    ThreadMXBean threads, List<GarbageCollectorMXBean> garbageCollectors,
                                    RuntimeMXBean runtime, MBeanServer mBeanServer) {
    super(memory, memoryPools, os, threads, garbageCollectors, runtime, mBeanServer);
    this.os = os;
  }

  /**
   * Returns the percentage of available file descriptors which are currently in use.
   *
   * @return the percentage of available file descriptors which are currently in use, or {@code
   *         NaN} if the running JVM does not have access to this information
   */
  @Override
  public double fileDescriptorUsage() {
    if (!(this.os instanceof UnixOperatingSystemMXBean)) {
      return Double.NaN;
    }
    Long openFds = ((UnixOperatingSystemMXBean)os).getOpenFileDescriptorCount();
    Long maxFds = ((UnixOperatingSystemMXBean)os).getMaxFileDescriptorCount();
    return openFds.doubleValue() / maxFds.doubleValue();
  }

}
