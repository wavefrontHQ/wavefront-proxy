package com.wavefront.examples;

import com.wavefront.integrations.metrics.WavefrontDropwizardReporter;
import io.dropwizard.metrics5.Counter;
import io.dropwizard.metrics5.MetricRegistry;

import java.util.concurrent.TimeUnit;

/**
 * Example for reporting dropwizard metrics into Wavefront via proxy.
 *
 * @author Vikram Raman
 */
public class ProxyReporting {
  public static void main(String args[]) throws InterruptedException {

    String host = args[0];
    int port = Integer.parseInt(args[1]);

    MetricRegistry registry = new MetricRegistry();
    Counter counter = registry.counter("proxy.dw5metric.foo.bar");

    WavefrontDropwizardReporter reporter = WavefrontDropwizardReporter.forRegistry(registry).
        withSource("app-1.company.com").
        withPointTag("pointkey1", "ptag1").
        withPointTag("pointkey2", "ptag2").
        build(host, port);
    reporter.start(5, TimeUnit.SECONDS);

    int i = 0;
    while (i++ < 40) {
      counter.inc(10);
      Thread.sleep(5000);
    }
    reporter.stop();
  }
}
