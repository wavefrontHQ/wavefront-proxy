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
cd metrics
mvn install
cd ../java/dropwizard-metrics/4.0/
mvn install
```

Then you will just need wavefront's `dropwizard-metrics-4.0` libraries that you have built as dependencies. Logging depends on `org.slf4j`:

```Maven
    <dependency>
        <groupId>com.wavefront</groupId>
        <artifactId>dropwizard-metrics-4.0</artifactId>
        <version>4.17</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>1.7.16</version>
    </dependency>
```

### Example Usage

The Wavefront reporter lets you use DropWizard metrics exactly as you normally would. See the [DropWizard getting started guide](https://dropwizard.github.io/metrics/3.1.0/getting-started/) for DropWizard basics. 

To create the Wavefront reporter:

- Import `com.wavefront.integrations.metrics.WavefrontReporter`
- Set the source using the `.withSource(String source)`
- Set the hostname and port of the Wavefront proxy using the `.build(String hostname, long port)` method. 
- Set point tags at the reporter level:
  - Add one point tag using `.withPointTag(String tagK, String tagV)`
  - Add one or more tags to a `Map<String,String>` and call `.withPointTags(Map)`
  
The reporter provides all the same options as [GraphiteReporter](http://metrics.dropwizard.io/3.1.0/manual/graphite/). By default:

  - There is no metric prefix
  - Rates are converted to seconds
  - Durations are converted to milliseconds
  - The filter is set to `MetricFilter.ALL`
  - The clock is set to `Clock.defaultClock()`
  
#### Example

The following code fragment creates a reporter that emits data every 10 seconds for:

- A `MetricRegistry` named `metrics`
- A Wavefront proxy on `localhost` at port `2878`
- Data that should appear with the source `app-1.company.com`
- Reporter-level point tags named `dc` and `service`
- Metric-level point tags named `type` and `counter`
- A counter metric named `requests`
- Various JVM metrics with prefix containing `jvm`


```
import com.wavefront.integrations.metrics.WavefrontReporter;
import io.dropwizard.metrics.Counter;
import io.dropwizard.metrics.MetricName;
import io.dropwizard.metrics.MetricRegistry;

MetricRegistry metrics = new MetricRegistry();   	
Map<String, String> tags = new HashMap<String,String>();  	
tags.put("type", "counter");
MetricName name = new MetricName("requests", tags);
Counter counter = metrics.counter(name);
    			
// If you specify the same tag name at the Metric and Reporter level,
// the Metric level tag will take precedence.
WavefrontReporter reporter = WavefrontReporter.forRegistry(metrics)
        .withSource("app-1.company.com")
        .withPointTag("dc", "dallas")
    	.withPointTag("service", "query")
    	.withJvmMetrics()
    	.prefixedWith("dropwizard")
    	.build("localhost", 2878);
    	
reporter.start(10, TimeUnit.SECONDS);
```

#### JVM Metrics

Default JVM metrics are added to the `MetricsRegistry` by calling `.withJvmMetrics()` when you create the reporter. If you call `.withJvmMetrics()`, the following metrics are added to the registry:

```
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
