package com.wavefront.agent.logforwarder.ingestion.client.gateway.metrics;

import com.google.common.net.HostAndPort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.dropwizard.metrics5.Gauge;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.MetricRegistry;
/**
 *  Only purpose of this class is to create a singleton around MetricRegistry and dw components which
 *  are accessible from any class at runtime
 *  TODO Revisit how to push metrics for monitoring consistent with other wf proxy metrics
 */
public class MetricsService {

  public static final String COUNT = "count";
  public static final String PAYLOADSIZE_IN_BYTES = "payloadsize-in-bytes";
  public static final String TIMETAKEN_IN_MICROS = "timetaken-in-micros";
  public static final String FAILURES = "failures";
  public static final String REJECTIONS = "rejections";
//  public static final String CANCELLATIONS = "cancellations";
//  private static final Logger logger =
//      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
//  private static final int WAVEFRONT_PORT = 2878;
//  private static final String WAVEFRONT_TAGS = "app:ops-ingest";
//  private static final String WAVEFRONT_METRICS_PREFIX = "wf-reporter";
//  private static final String KAFKA_CONSUMER_METRICS_PREFIX = WAVEFRONT_METRICS_PREFIX + ".KafkaConsumer";
//
  private static MetricsService instance = new MetricsService();
  public final MetricRegistry primaryRegistry;
  public final MetricRegistry tenantRegistry;
  public final MetricRegistry consumerRegistry;
//  private WavefrontReporter wfReporter;
//  private WavefrontReporter tenantsWFReporter;
//  private WavefrontReporter consumerWFReporter;
//
  private MetricsService() {
    String wavefrontProxy = System.getProperty("wavefront-proxy");
//
    this.primaryRegistry = new MetricRegistry();
    this.tenantRegistry = new MetricRegistry();
    this.consumerRegistry = new MetricRegistry();
    //TODO Now you are running inside wf proxy look at how to ingest metrics internal process
//
//    if (wavefrontProxy != null && !wavefrontProxy.isEmpty()) {
//      HostAndPort proxy =
//          HostAndPort.fromString(wavefrontProxy).withDefaultPort(WAVEFRONT_PORT);
//      wfReporter = WavefrontReporter.forRegistry(primaryRegistry).withSource(getHostName())
//          .withJvmMetrics().withPointTags(parseMetricTags())
//          .prefixedWith(WAVEFRONT_METRICS_PREFIX)
//          .build(proxy.getHost(), proxy.getPort());
//      tenantsWFReporter = WavefrontReporter.forRegistry(tenantRegistry)
//          .withSource(getHostName()).withJvmMetrics().withPointTags(parseMetricTags())
//          .prefixedWith(WAVEFRONT_METRICS_PREFIX)
//          .build(proxy.getHost(), proxy.getPort());
//      consumerWFReporter = WavefrontReporter.forRegistry(consumerRegistry).withSource(getHostName())
//          .withPointTags(parseMetricTags())
//          .prefixedWith(KAFKA_CONSUMER_METRICS_PREFIX)
//          .build(proxy.getHost(), proxy.getPort());
//    }
  }

  public static MetricsService getInstance() {
    return instance;
  }
//
//  public void counter(String name, MetricSupplier<Counter> supplier) {
//    primaryRegistry.counter(name, supplier);
//  }
//
//  public <T extends Metric> void register(String registryName, T metric, String... names) {
//    primaryRegistry.register(registryName, metric);
//  }
//
  public Meter getMeter(String meterName) {
    return primaryRegistry.meter(meterName);
  }
//
//  public Meter getTenantMeter(String meterName) {
//    return tenantRegistry.meter(meterName);
//  }
//
  public Histogram getHistogram(String histogramName) {
    return primaryRegistry.histogram(histogramName);
  }
//
//  public Histogram getTenantHistogram(String histogramName) {
//    return tenantRegistry.histogram(histogramName);
//  }
//
//  public void stop() {
//    if (wfReporter != null) {
//      wfReporter.stop();
//    }
//    if (tenantsWFReporter != null) {
//      tenantsWFReporter.stop();
//    }
//    if (consumerWFReporter != null) {
//      consumerWFReporter.stop();
//    }
//  }
//
//  public void start() {
//    if (wfReporter != null) {
//      wfReporter.start(20, TimeUnit.SECONDS);
//    }
//    if (tenantsWFReporter != null) {
//      tenantsWFReporter.start(60, TimeUnit.SECONDS);
//    }
//    if (consumerWFReporter != null) {
//      consumerWFReporter.start(60, TimeUnit.SECONDS);
//    }
//  }
//
//  public void startMetricsPeriodicLogger() {
//    Logger logger = LoggerFactory.getLogger("METRICS_LOGGER");
//
//    Slf4jReporter primaryReporter = Slf4jReporter.forRegistry(primaryRegistry).outputTo(logger)
//        .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
//    primaryReporter.start(1, TimeUnit.MINUTES);
//
//    Slf4jReporter tenantReporter = Slf4jReporter.forRegistry(tenantRegistry).outputTo(logger)
//        .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
//    tenantReporter.start(1, TimeUnit.MINUTES);
//
//    Slf4jReporter consumerReporter = Slf4jReporter.forRegistry(consumerRegistry).outputTo(logger)
//        .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
//    consumerReporter.start(1, TimeUnit.MINUTES);
//  }
//
//  private String getHostName() {
//    String hostname = "ops-ingest.svc.default.local";
//    try {
//      hostname = InetAddress.getLocalHost().getHostName();
//    } catch (UnknownHostException e) {
//      // assume default and carry on.
//      logger.debug(e.getMessage(), e);
//    }
//    return hostname;
//  }
//
//  // example tag: "product:ops-ingest,env:local,namespace:default"
//  private Map<String, String> parseMetricTags() {
//    String tags = System.getProperty("wavefront-tags", WAVEFRONT_TAGS);
//    return Stream.of(tags.split(",")).map(s -> s.split(":"))
//        .collect(Collectors.toMap(m -> m[0], m -> m[1]));
//  }
//
//  public static class CustomGauge<T> implements Gauge<T> {
//    private volatile T val;
//
//    @Override
//    public T getValue() {
//      return val;
//    }
//
//    public void setValue(T val) {
//      this.val = val;
//    }
//  }
//
//  public static class CustomMetricSupplier<T> implements MetricRegistry.MetricSupplier<Gauge> {
//
//    @Override
//    public Gauge<T> newMetric() {
//      return new CustomGauge<T>();
//    }
//  }
}
