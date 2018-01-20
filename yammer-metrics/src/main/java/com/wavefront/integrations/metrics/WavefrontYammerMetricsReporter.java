package com.wavefront.integrations.metrics;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.common.MetricsToTimeseries;
import com.wavefront.common.Pair;
import com.wavefront.metrics.MetricTranslator;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class WavefrontYammerMetricsReporter extends AbstractPollingReporter {

  protected static final Logger logger = Logger.getLogger(WavefrontYammerMetricsReporter.class.getCanonicalName());

  private static final Clock clock = Clock.defaultClock();
  private static final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();

  private final boolean includeJvmMetrics;
  private final ConcurrentHashMap<String, Double> gaugeMap;
  private final SocketMetricsProcessor socketMetricProcessor;
  private final MetricTranslator metricTranslator;

  private int metricsGeneratedLastPass = 0;  /* How many metrics were emitted in the last call to run()? */

  public WavefrontYammerMetricsReporter(MetricsRegistry metricsRegistry, String name, String hostname, int port,
                                        int wavefrontHistogramPort, Supplier<Long> timeSupplier) throws IOException {
    this(metricsRegistry, name, hostname, port, wavefrontHistogramPort, timeSupplier, false, null, false, false);
  }

  public WavefrontYammerMetricsReporter(MetricsRegistry metricsRegistry, String name, String hostname, int port,
                                        int wavefrontHistogramPort, Supplier<Long> timeSupplier,
                                        boolean prependGroupName,
                                        @Nullable MetricTranslator metricTranslator,
                                        boolean includeJvmMetrics,
                                        boolean clearMetrics) throws IOException {
    this(metricsRegistry, name, hostname, port, wavefrontHistogramPort, timeSupplier, prependGroupName,
        metricTranslator, includeJvmMetrics, clearMetrics, true, true);
  }

  /**
   * Reporter of a Yammer metrics registry to Wavefront.
   *
   * @param metricsRegistry        The registry to scan-and-report
   * @param name                   A human readable name for this reporter
   * @param hostname               The remote host where the wavefront proxy resides
   * @param port                   Listening port on Wavefront proxy of graphite-like telemetry data
   * @param wavefrontHistogramPort Listening port for Wavefront histogram data
   * @param timeSupplier           Get current timestamp, stubbed for testing
   * @param prependGroupName       If true, outgoing telemetry is of the form "group.name" rather than "name".
   * @param metricTranslator       If present, applied to each MetricName/Metric pair before flushing to Wavefront. This
   *                               is useful for adding point tags. Warning: this is called once per metric per scan, so
   *                               it should probably be performant. May be null.
   * @param clearMetrics           If true, clear histograms and timers per flush.
   * @param sendZeroCounters       Whether counters with a value of zero is sent across.
   * @param sendEmptyHistograms    Whether empty histograms are sent across the wire.
   * @param includeJvmMetrics      Whether JVM metrics are automatically included.
   * @throws IOException When we can't remotely connect to Wavefront.
   */
  public WavefrontYammerMetricsReporter(MetricsRegistry metricsRegistry, String name, String hostname, int port,
                                        int wavefrontHistogramPort, Supplier<Long> timeSupplier,
                                        boolean prependGroupName,
                                        @Nullable MetricTranslator metricTranslator,
                                        boolean includeJvmMetrics,
                                        boolean clearMetrics,
                                        boolean sendZeroCounters,
                                        boolean sendEmptyHistograms) throws IOException {
    super(metricsRegistry, name);
    this.metricTranslator = metricTranslator;
    this.socketMetricProcessor = new SocketMetricsProcessor(hostname, port, wavefrontHistogramPort, timeSupplier,
        prependGroupName, clearMetrics, sendZeroCounters, sendEmptyHistograms);
    this.includeJvmMetrics = includeJvmMetrics;
    this.gaugeMap = new ConcurrentHashMap<>();
  }

  private void upsertGauges(String metricName, Double t) {
    gaugeMap.put(metricName, t);

    // This call to newGauge only mutates the metrics registry the first time through. Thats why it's important
    // to access gaugeMap indirectly, as opposed to counting on new calls to newGauage to replace the underlying
    // double supplier.
    getMetricsRegistry().newGauge(
        new MetricName("", "", MetricsToTimeseries.sanitize(metricName)),
        new Gauge<Double>() {
          @Override
          public Double value() {
            return gaugeMap.get(metricName);
          }
        });
  }

  private void upsertGauges(String base, Map<String, Double> metrics) {
    for (Map.Entry<String, Double> entry : metrics.entrySet()) {
      upsertGauges(base + "." + entry.getKey(), entry.getValue());
    }
  }

  private void upsertJavaMetrics() {
    upsertGauges("jvm.memory", MetricsToTimeseries.memoryMetrics(vm));
    upsertGauges("jvm.buffers.direct", MetricsToTimeseries.buffersMetrics(vm.getBufferPoolStats().get("direct")));
    upsertGauges("jvm.buffers.mapped", MetricsToTimeseries.buffersMetrics(vm.getBufferPoolStats().get("mapped")));
    upsertGauges("jvm.thread-states", MetricsToTimeseries.threadStateMetrics(vm));
    upsertGauges("jvm", MetricsToTimeseries.vmMetrics(vm));
    upsertGauges("current_time", (double) clock.time());
    for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
      upsertGauges("jvm.garbage-collectors." + entry.getKey(), MetricsToTimeseries.gcMetrics(entry.getValue()));
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
      if (includeJvmMetrics) upsertJavaMetrics();

      for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics().entrySet()) {
        for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
          MetricName metricName = subEntry.getKey();
          Metric metric = subEntry.getValue();
          if (metricTranslator != null) {
            Pair<MetricName, Metric> pair = metricTranslator.apply(Pair.of(metricName, metric));
            if (pair == null) continue;
            metricName = pair._1;
            metric = pair._2;
          }
          metric.processWith(socketMetricProcessor, metricName, null);
          metricsGeneratedLastPass++;
        }
      }
      socketMetricProcessor.flush();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Cannot report point to Wavefront! Trying again next iteration.", e);
    }
  }
}
