# Wavefront Proxy

The Wavefront proxy is a light-weight Java application that you send your metrics to. It handles authentication and the transmission of your metrics to your Wavefront instance.

Source code under org.logstash.* is used from
 [logstash-input-beats](https://github.com/logstash-plugins/logstash-input-beats) via the Apache 2.0 license.

## Installation

### Using The Wavefront Installer

The recommended (and by far the easiest) way to install the most recent release of the proxy is to use [The Wavefront Installer](https://community.wavefront.com/docs/DOC-1161) - we've developed a simple, one-line installer that configures the Wavefront proxy and/or collectd to send metrics to Wavefront in as little as one step.

### Using Linux Packages

We have pre-build packages for popular Linux distros. Packages for released versions are available at https://packagecloud.io/wavefrontHQ/proxy, release candidate versions are available at https://packagecloud.io/wavefront/proxy-next.

### Building your own

To build your own version, run the following commands (you need [Apache Maven](https://maven.apache.org) installed for a successful build)

```
git clone https://github.com/wavefrontHQ/java
cd java
mvn install
```

## Configuration

For the detailed list of configuration options, please refer to [Wavefront Production Proxy Configuration Guide](https://community.wavefront.com/docs/DOC-1034) on our Community site.
