# Wavefront Reporter

This is a plug-in Wavefront Reporter for 1.2.x version of [Dropwizard](https://github.com/dropwizard/dropwizard).

Adding a dependency to your Dropwizard project gives you a new metric reporter type `wavefront` (similar to `graphite`) that can be configured entirely in the application's YAML config file without writing a single line of code. 

## Usage

This Reporter sends data to Wavefront via a proxy. You can easily install the proxy by following [these instructions](https://docs.wavefront.com/proxies_installing.html).

To use the Reporter you'll need to know the hostname and port (which by default is localhost:2878) where the Wavefront proxy is running.

It is designed to be used with the [stable version 1.2.x of Dropwizard](https://github.com/dropwizard/dropwizard).

### Setting up Maven

You will also need `org.slf4j` for logging:

```Maven
    <dependency>
      <groupId>com.wavefront</groupId>
      <artifactId>dropwizard-metrics-wavefront</artifactId>
      <version>[LATEST VERSION]</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>1.7.16</version>
    </dependency>
```

### Example Usage

The Wavefront Reporter lets you use DropWizard metrics exactly as you normally would. See its [getting started guide](https://dropwizard.github.io/metrics/3.1.0/getting-started/) if you haven't used it before.

It simply gives you a new Reporter that will seamlessly work with Wavefront. For instance, to create a Reporter which will emit data every 30 seconds to:

- A Wavefront proxy on `localhost` at port `2878`
- Data that should appear in Wavefront under `source=app-1.company.com`
- Two point tags named `dc` and `service`

you would add something like this to your application's YAML config file:

```yaml
metrics:
  reporters:
    - type: wavefront
      proxyHost: localhost
      proxyPort: 2878
      metricSource: app1.company.com
      prefix: dropwizard
      frequency: 30s
      pointTags:
        dc: dallas
        service: query
```

If you don't specify `metricSource`, it will try to do a reverse DNS lookup for the local host and use the DNS name.

