# Wavefront dropwizard-metrics Reporter

This is a Wavefront Reporter for the Stable version of [Dropwizard Metrics](https://dropwizard.github.io/metrics/3.1.0/) (formerly Coda Hale & Yammer Metrics).

It sends data to the Wavefront service via a Proxy and supports Point Tags being assigned at the Reporter level.

## Usage

This Reporter sends data via to Wavefront via a Proxy. Version 3.5 or later is required. You can easily install the Proxy by following [these instructions](https://github.com/wavefrontHQ/install).

To use the Reporter you'll need to know the hostname and port (which by default is 2878) where the Wavefront Proxy is running.

It is designed to be used with the [stable version 3.1.x of Dropwizard Metrics](https://dropwizard.github.io/metrics/3.1.0/getting-started/).

### Setting up Maven

You will need both the DropWizard `metrics-core` and the Wavefront `metrics-wavefront` libraries as dependencies. Logging depends on `org.slf4j`:

```Maven
   <dependency>
      <groupId>io.dropwizard.metrics</groupId>
      <artifactId>metrics-core</artifactId>
      <version>3.1.2</version>
    </dependency>
    <dependency>
      <groupId>com.wavefront</groupId>
      <artifactId>dropwizard-metrics-3.1</artifactId>
      <version>3.8</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>1.7.16</version>
    </dependency>       
```

Versions `3.1.0` and `3.1.1` of `metrics-core` will also work.

### Example Usage

The Wavefront Reporter lets you use DropWizard metrics exactly as you normally would. See its [getting started guide](https://dropwizard.github.io/metrics/3.1.0/getting-started/) if you haven't used it before.

It simply gives you a new Reporter that will seamlessly work with Wavefront. First `import com.wavefront.integrations.metrics.WavefrontReporter;`

Then for example to create a Reporter which will emit data every 10 seconds for:

- A `MetricsRegistry` named `metrics`
- A Wavefront Proxy on `localhost` at port `2878`
- Data that should appear in Wavefront as `source=app-1.company.com`
- Two point tags named `dc` and `service`

you would do something like this:

```java
WavefrontReporter reporter = WavefrontReporter.forRegistry(metrics)
        .withSource("app-1.company.com")
    	.withPointTag("dc", "dallas")
    	.withPointTag("service", "query")
    	.build("localhost", 2878);
```

You must provide the source using the `.withSource(String source)` method and pass the Hostname and Port of the Wavefront Proxy using the `.build(String hostname, long port)` method.

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
