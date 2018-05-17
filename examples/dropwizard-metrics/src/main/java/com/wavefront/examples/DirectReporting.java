package com.wavefront.examples;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.wavefront.integrations.metrics.WavefrontReporter;

/**
 * Example for reporting dropwizard metrics into Wavefront via Direct Ingestion.
 *
 * @author Vikram Raman
 */
public class DirectReporting {

  public static void main(String args[]) throws InterruptedException {

    String server = args[0];
    String token = args[1];

    MetricRegistry registry = new MetricRegistry();
    Counter counter = registry.counter("direct.metric.foo.bar");

    WavefrontReporter reporter = WavefrontReporter.forRegistry(registry).
        withSource("app-1.company.com").
        withPointTag("dc", "dallas").
        withPointTag("service", "query").
        buildDirect(server, token);
    reporter.start(5, TimeUnit.SECONDS);

    int i = 0;
    while (i++ < 30) {
      counter.inc(10);
      Thread.sleep(1000);
    }
    reporter.stop();
  }
}
