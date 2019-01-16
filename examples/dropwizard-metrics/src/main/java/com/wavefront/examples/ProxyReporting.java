package com.wavefront.examples;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.wavefront.integrations.metrics.WavefrontReporter;

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
    Counter counter = registry.counter("proxy.metric.foo.bar");

    WavefrontReporter reporter = WavefrontReporter.forRegistry(registry).
        withSource("app-1.company.com").
        withPointTag("dc", "dallas").
        withPointTag("service", "query").
        build(host, port);
    reporter.start(5, TimeUnit.SECONDS);

    int i = 0;
    while (i++ < 30) {
      counter.inc(10);
      Thread.sleep(1000);
    }
    reporter.stop();
  }
}
