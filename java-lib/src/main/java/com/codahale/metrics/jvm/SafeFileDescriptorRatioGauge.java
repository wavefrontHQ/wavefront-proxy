package com.codahale.metrics.jvm;

import com.codahale.metrics.RatioGauge;
import com.sun.management.UnixOperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

/**
 * Java 9 compatible implementation of FileDescriptorRatioGauge that doesn't use reflection
 * and is not susceptible to an InaccessibleObjectException
 *
 * The gauge represents a ratio of used to total file descriptors.
 *
 * @author Vasily Vorontsov (vasily@wavefront.com)
 */
public class SafeFileDescriptorRatioGauge extends RatioGauge {
  private final OperatingSystemMXBean os;

  /**
   * Creates a new gauge using the platform OS bean.
   */
  public SafeFileDescriptorRatioGauge() {
    this(ManagementFactory.getOperatingSystemMXBean());
  }

  /**
   * Creates a new gauge using the given OS bean.
   *
   * @param os    an {@link OperatingSystemMXBean}
   */
  public SafeFileDescriptorRatioGauge(OperatingSystemMXBean os) {
    this.os = os;
  }

  /**
   * @return  {@link com.codahale.metrics.RatioGauge.Ratio of used to total file descriptors.}
   */
  protected Ratio getRatio() {
    if (!(this.os instanceof UnixOperatingSystemMXBean)) {
      return Ratio.of(Double.NaN, Double.NaN);
    }
    Long openFds = ((UnixOperatingSystemMXBean)os).getOpenFileDescriptorCount();
    Long maxFds = ((UnixOperatingSystemMXBean)os).getMaxFileDescriptorCount();
    return Ratio.of(openFds.doubleValue(), maxFds.doubleValue());
  }
}
