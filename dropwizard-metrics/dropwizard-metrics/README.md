# Wavefront Reporter

This is a Wavefront Reporter compatible with versions 3.1.x, 3.2.x and 4.0.x of [Dropwizard Metrics](https://metrics.dropwizard.io/) (formerly Coda Hale & Yammer Metrics).

It sends data to the Wavefront service using the [Wavefront proxy](https://docs.wavefront.com/proxies.html) or using [direct ingestion](https://docs.wavefront.com/direct_ingestion.html) and supports point tags being assigned at the Reporter level.

## Usage

The Wavefront Reporter lets you use Dropwizard metrics exactly as you normally would. See its [getting started guide](https://dropwizard.github.io/metrics/3.1.0/getting-started/) if you haven't used it before.

It simply gives you a new Reporter that will work seamlessly with Wavefront. Set up Maven as given below, `import com.wavefront.integrations.metrics.WavefrontReporter;` and then report to a Wavefront proxy or directly to a Wavefront server.

### Setting up Maven

You will need both the DropWizard `metrics-core` and the Wavefront `dropwizard-metrics` libraries as dependencies. Logging depends on `org.slf4j`:

```Maven
   <dependency>
      <groupId>io.dropwizard.metrics</groupId>
      <artifactId>metrics-core</artifactId>
      <version>3.2.5</version>
    </dependency>
    <dependency>
      <groupId>com.wavefront</groupId>
      <artifactId>dropwizard-metrics</artifactId>
      <version>[LATEST VERSION]</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>1.7.16</version>
    </dependency>       
```

Versions `3.1.x`, `3.2.x` and `4.0.x` of `metrics-core` will work.

### Report to a Wavefront Proxy

You can install the proxy by following [these instructions](https://docs.wavefront.com/proxies_installing.html).
To use the Reporter you'll need to provide the hostname and port (which by default is 2878) of the Wavefront proxy.

For example to create a Reporter which will emit data to a Wavefront proxy every 5 seconds:

```java
MetricRegistry registry = new MetricRegistry();
Counter evictions = registry.counter("cache-evictions");

String hostname = "wavefront.proxy.hostname";
int port = 2878;

WavefrontReporter reporter = WavefrontReporter.forRegistry(registry).
    withSource("app-1.company.com").
    withPointTag("dc", "us-west-2").
    withPointTag("service", "query").
    build(hostname, port);
reporter.start(5, TimeUnit.SECONDS);
```

### Report to a Wavefront Server

You can send metrics directly to a Wavefront service. To use the Reporter you'll need to provide the Wavefront server URL and a token with direct data ingestion permission.

For example to create a Reporter which will emit data to a Wavefront server every 5 seconds:

```java
MetricRegistry registry = new MetricRegistry();
Counter evictions = registry.counter("cache-evictions");

String server = "https://<instance>.wavefront.com";
String token = "<valid_wavefront_token>";

WavefrontReporter reporter = WavefrontReporter.forRegistry(registry).
    withSource("app-1.company.com").
    withPointTag("dc", "us-west-2").
    withPointTag("service", "query").
    buildDirect(server, token);
reporter.start(5, TimeUnit.SECONDS);
```

### Extended Usage

The Reporter provides all the same options that the [GraphiteReporter](http://metrics.dropwizard.io/3.1.0/manual/graphite/) does. By default:

- There is no prefix on the Metrics
- Rates will be converted to Seconds
- Durations will be converted to Milliseconds
- `MetricFilter.ALL` will be used for the Filter
- `Clock.defaultClock()` will be used for the Clock

In addition you can also:

- Supply point tags for the Reporter to use. There are two ways to specify point tags at the Reporter level, individually using `.withPointTag(String tagK, String tagV)` or create a `Map<String,String>` and call `.withPointTags(my-map)` to do many at once.
- Call `.withJvmMetrics()` when building the Reporter if you want it to add some default JVM metrics to the given MetricsRegistry

If `.withJvmMetrics()` is used the following metrics will be added to the registry:

```java
registry.register("jvm.uptime", new Gauge<Long>() {
    @Override
	public Long getValue() {
	    return ManagementFactory.getRuntimeMXBean().getUptime();
	}
});
registry.register("jvm.current_time", new Gauge<Long>() {
    @Override
	public Long getValue() {
	    return clock.getTime();
    }
});

registry.register("jvm.classes", new ClassLoadingGaugeSet());
registry.register("jvm.fd_usage", new FileDescriptorRatioGauge());
registry.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
registry.register("jvm.gc", new GarbageCollectorMetricSet());
registry.register("jvm.memory", new MemoryUsageGaugeSet());
```
