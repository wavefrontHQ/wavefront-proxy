# Wavefront Proxy
[![Javadocs](http://javadoc.io/badge/com.wavefront/proxy.svg)](http://javadoc.io/doc/com.wavefront/proxy)

The Wavefront proxy is a light-weight Java application that you send your metrics, histograms, and trace data to. It handles authentication and the transmission of your data to your Wavefront instance.

Source code under `org.logstash.*` is used from [logstash-input-beats](https://github.com/logstash-plugins/logstash-input-beats) via the Apache 2.0 license.

## Proxy Installation Options

### Option 1. Use The Wavefront Installer

The recommended (and by far the easiest) way to install the most recent release of the proxy is to use [the Wavefront installer](https://docs.wavefront.com/proxies_installing.html##proxy-installation). This is a simple, one-line installer that configures the Wavefront proxy and/or `collectd` to send telemetry data to Wavefront in as little as one step.

### Option 2. Use Linux Packages

We have pre-build packages for popular Linux distros. 
* Packages for released versions are available at https://packagecloud.io/wavefront/proxy. 
* Release candidate versions are available at https://packagecloud.io/wavefront/proxy-next.

### Option 3. Build Your Own Proxy

To build your own version, run the following commands (you need [Apache Maven](https://maven.apache.org) installed for a successful build).

```
git clone https://github.com/wavefrontHQ/wavefront-proxy
cd wavefront-proxy
mvn clean install
```

## Setting Up a Wavefront Proxy
To set up a Wavefront proxy to listen for metrics, histograms, and trace data:
1.  On the host that will run the proxy, install using one of the above methods (using [Wavefront installer](http://docs.wavefront.com/proxies_installing.html##proxy-installation) is the easiest).
    * If you already have an installed proxy, you may need to [upgrade](http://docs.wavefront.com/proxies_installing.html#upgrading-a-proxy) it. You need Version 4.33 or later to listen for trace data.
2. On the proxy host, open the proxy configuration file `wavefront.conf` for editing. The file location depends on the host:
    * Linux - `/etc/wavefront/wavefront-proxy/wavefront.conf` 
    * Mac - `/usr/local/etc/wavefront/wavefront-proxy/wavefront.conf`
    * Windows - `C:\Program Files (x86)\Wavefront\conf\wavefront.conf`
    * Additional paths may be listed [here](http://docs.wavefront.com/proxies_configuring.html#paths).
3. In the `wavefront.conf` file, find and uncomment the property for each listener port you want to enable. You must enable at least one listener port. 
    * The following example enables the default or recommended listener ports for metrics, histogram distributions, and trace data.  
    ```
    ## wavefront.conf file
    ...
    # Listens for metric data. Default: 2878
    pushListenerPorts=2878
    ...
    # Listens for histogram distributions. 
    # Recommended: 2878 (proxy version 4.29 or later) or 40000 (earlier proxy versions)
    histogramDistListenerPorts=2878
    ...
    # Listens for trace data. Recommended: 30000
    traceListenerPorts=30000
    ```
4. Save the `wavefront.conf` file.
5. [Start](http://docs.wavefront.com/proxies_installing.html###starting-and-stopping-a-proxy) the proxy.


## Advanced Proxy Configuration

You can find [advanced proxy configuration information](https://docs.wavefront.com/proxies_configuring.html) on our docs site.
