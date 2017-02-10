package com.wavefront.integrations.metrics;

import com.google.common.base.Joiner;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.WavefrontHistogram;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Driver for basic experimentation with a {@link com.wavefront.integrations.metrics.WavefrontYammerMetricsReporter}
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class Main {

  public static void main(String[] args) throws IOException, InterruptedException {
    // Parse inputs.
    System.out.println("Args: " + Joiner.on(", ").join(args));
    if (args.length != 2) {
      System.out.println("Usage: java -jar this.jar <metricsPort> <histogramsPort>");
      return;
    }
    int port = Integer.parseInt(args[0]);
    int histoPort = Integer.parseInt(args[1]);

    // Set up periodic reporting.
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    WavefrontYammerMetricsReporter wavefrontYammerMetricsReporter = new WavefrontYammerMetricsReporter(metricsRegistry,
        "wavefrontYammerMetrics", "localhost", port, histoPort, System::currentTimeMillis);
    wavefrontYammerMetricsReporter.start(5, TimeUnit.SECONDS);

    // Populate test metrics.
    Counter counter = metricsRegistry.newCounter(new TaggedMetricName("group", "mycounter", "tag1", "value1"));
    Histogram histogram = metricsRegistry.newHistogram(new TaggedMetricName("group2", "myhisto"), false);
    WavefrontHistogram wavefrontHistogram = WavefrontHistogram.get(metricsRegistry,
        new TaggedMetricName("group", "mywavefronthisto", "tag2", "value2"));

    while (true) {
      counter.inc();
      histogram.update(counter.count());
      wavefrontHistogram.update(counter.count());
      Thread.sleep(1000);
    }
  }

}
