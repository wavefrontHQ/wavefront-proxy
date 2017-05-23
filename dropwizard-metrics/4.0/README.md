# Wavefront Reporter

This is a Wavefront Reporter for latest Master (4.0) of [Dropwizard Metrics](https://github.com/dropwizard/metrics) (formerly Coda Hale & Yammer Metrics).

It sends data to the Wavefront service via a proxy and supports point tags being assigned both at the Reporter level _and_ the Metric level.

## Usage

This Reporter sends data via to Wavefront via a proxy. Version 3.5 or later is required. You can easily install the proxy by following [these instructions](https://github.com/wavefrontHQ/install).

To use the Reporter you'll need to know the hostname and port (which by default is 2878) where the Wavefront proxy is running.

It is designed to be used with [the 4.0.0-SNAPSHOT of Dropwizard Metrics](https://github.com/dropwizard/metrics).

### Setting up Maven

Until DropWizard releases a 4.0.0 version you won't be able to use this directly from Maven Central. Instead you must first build DropWizard, then this project locally. This is pretty straightforward:

```sh
git clone https://github.com/dropwizard/metrics.git
git clone https://github.com/wavefrontHQ/java.git
cd metrics/metrics-core
mvn install
cd ../metrics-jvm/
mvn install
cd ../../java/dropwizard-metrics/4.0/
mvn install
```

Then you will just need both the DropWizard `metrics-core` and the Wavefront `metrics-wavefront` libraries as dependencies in your project. Logging depends on `org.slf4j`:

```Maven
   <dependency>
      <groupId>io.dropwizard.metrics</groupId>
      <artifactId>metrics-core</artifactId>
      <version>4.0.0-SNAPSHOT</version>
    </dependency>
    <dependency>
      <groupId>com.wavefront</groupId>
      <artifactId>dropwizard-metrics-4.0</artifactId>
      <version>3.9-SNAPSHOT</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>1.7.16</version>
    </dependency>
```

### Example Usage

The Wavefront Reporter lets you use DropWizard metrics exactly as you normally would. See its [getting started guide](https://dropwizard.github.io/metrics/3.1.0/getting-started/) if you haven't used it before.

It simply gives you a new Reporter that will seamlessly work with Wavefront. First `import com.wavefront.integrations.metrics.WavefrontReporter;`

Then for example to create a Reporter which will emit data every 10 seconds for:

- A `MetricsRegistry` named `metrics` with a Counter that has a tag named `type`
- A Wavefront proxy on `localhost` at port `2878`
- Data that should appear in Wavefront as `source=app-1.company.com`
- Two point tags named `dc` and `service` on the Reporter
- All metrics in Wavefront should be prefixed with "dropwizard."

you would do something like this:

```java
MetricRegistry metrics = new MetricRegistry();   	
Map<String, String> tags = new HashMap<String,String>();  	
tags.put("type", "counter");
MetricName name = new MetricName("requests", tags);
Counter counter = metrics.counter(name);
    			
// NB If you specify the same tag name at the Metric and Reporter 
// level the Metric level one will overwrite it
WavefrontReporter reporter = WavefrontReporter.forRegistry(metrics)
        .withSource("app-1.company.com")
        .withPointTag("dc", "dallas")
    	.withPointTag("service", "query")
    	.prefixedWith("dropwizard")
    	.build("localhost", 2878);
    	
reporter.start(10, TimeUnit.SECONDS);
```

You must provide the source using the `.withSource(String source)` method and pass the Hostname and Port of the Wavefront proxy using the `.build(String hostname, long port)` method.

The Reporter provides all the same options that the [GraphiteReporter](http://metrics.dropwizard.io/3.1.0/manual/graphite/) does. By default:

- There is no prefix on the Metrics
- Rates will be converted to Seconds
- Durations will be converted to Milliseconds
- `MetricFilter.ALL` will be used for the Filter
- `Clock.defaultClock()` will be used for the Clock

In addition you can also:

- Supply point tags for the Reporter to use. There are two ways to specify point tags at the Reporter level, individually using `.withPointTag(String tagK, String tagV)` or create a `Map<String,String>` and call `.withPointTags(my-map)` to do many at once. Note that if you specify the same tag name at the Metric and Reporter level the Metric level one will overwrite it.
- Call `.withJvmMetrics()` when building the Reporter if you want it to add some default JVM metrics to the given MetricsRegistry.

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
