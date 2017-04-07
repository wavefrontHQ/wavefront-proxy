package com.wavefront.integrations.metrics;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.common.MetricsToTimeseries;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class WavefrontYammerMetricsReporter extends AbstractPollingReporter {

  protected static final Logger logger = Logger.getLogger(WavefrontYammerMetricsReporter.class.getCanonicalName());
  private SocketMetricsProcessor socketMetricProcessor;
  private Function<MetricName, MetricName> transformer;
  private int metricsGeneratedLastPass = 0;  /* How many metrics were emitted in the last call to run()? */
  private final boolean includeJvmMetrics;
  private static final Clock clock = Clock.defaultClock();
  private static final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();

  public WavefrontYammerMetricsReporter(MetricsRegistry metricsRegistry, String name, String hostname, int port,
                                        int wavefrontHistogramPort, Supplier<Long> timeSupplier) throws IOException {
    this(metricsRegistry, name, hostname, port, wavefrontHistogramPort, timeSupplier, false, null, false);
  }

  /**
   * Reporter of a Yammer metrics registry to Wavefront
   *
   * @param metricsRegistry        The registry to scan-and-report
   * @param name                   A human readable name for this reporter
   * @param hostname               The remote host where the wavefront proxy resides
   * @param port                   Listening port on Wavefront proxy of graphite-like telemetry data
   * @param wavefrontHistogramPort Listening port for Wavefront histogram data
   * @param timeSupplier           Get current timestamp, stubbed for testing
   * @param prependGroupName       If true, outgoing telemetry is of the form "group.name" rather than "name".
   * @param transformer            If present, applied to each MetricName before flushing to Wavefront. This is useful
   *                               for adding point tags. Warning: this is called once per metric per scan, so it should
   *                               probably be performant.
   * @throws IOException When we can't remotely connect to Wavefront.
   */
  public WavefrontYammerMetricsReporter(MetricsRegistry metricsRegistry, String name, String hostname, int port,
                                        int wavefrontHistogramPort, Supplier<Long> timeSupplier,
                                        boolean prependGroupName,
                                        @Nullable Function<MetricName, MetricName> transformer,
                                        boolean includeJvmMetrics) throws IOException {
    super(metricsRegistry, name);
    this.transformer = transformer;
    this.socketMetricProcessor = new SocketMetricsProcessor(hostname, port, wavefrontHistogramPort, timeSupplier,
        prependGroupName);
    this.includeJvmMetrics = includeJvmMetrics;
  }

  private <T> void setGauge(String metricName, T t) {
    getMetricsRegistry().<T>newGauge(new MetricName("", "", metricName), new Gauge<T>() {
      @Override
      public T value() {
        return t;
      }
    });
  }

  private void setGauges(String base, Map<String, Double> metrics) {
    for (Map.Entry<String, Double> entry : metrics.entrySet()) {
      setGauge(base + "." + entry.getKey(), entry.getValue());
    }
  }

  private void setJavaMetrics() {
    setGauges("jvm.memory", MetricsToTimeseries.memoryMetrics(vm));
    setGauges("jvm.buffers.direct", MetricsToTimeseries.buffersMetrics(vm.getBufferPoolStats().get("direct")));
    setGauges("jvm.buffers.mapped", MetricsToTimeseries.buffersMetrics(vm.getBufferPoolStats().get("mapped")));
    setGauges("jvm.thread-states", MetricsToTimeseries.threadStateMetrics(vm));
    setGauges("jvm", MetricsToTimeseries.vmMetrics(vm));
    setGauge("current_time", clock.time());
    for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
      setGauges("jvm.garbage-collectors." + entry.getKey(), MetricsToTimeseries.gcMetrics(entry.getValue()));
    }
  }

  /**
   * @return How many metrics were processed during the last call to {@link #run()}.
   */
  @VisibleForTesting
  int getMetricsGeneratedLastPass() {
    return metricsGeneratedLastPass;
  }

  @Override
  public void run() {
    metricsGeneratedLastPass = 0;
    try {
      if (includeJvmMetrics) setJavaMetrics();

      for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics().entrySet()) {
        for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
          MetricName metricName = subEntry.getKey();
          if (transformer != null) {
            metricName = transformer.apply(metricName);
          }
          subEntry.getValue().processWith(socketMetricProcessor, metricName, null);
          metricsGeneratedLastPass++;
        }
      }
      socketMetricProcessor.flush();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Cannot report point to Wavefront! Trying again next iteration.", e);
    }
  }
}
