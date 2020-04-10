# Wavefront Proxy Project [![Build Status](https://travis-ci.org/wavefrontHQ/wavefront-proxy.svg?branch=master)](https://travis-ci.org/wavefrontHQ/wavefront-proxy)

[Wavefront](https://docs.wavefront.com/) is a high-performance streaming analytics platform for monitoring and optimizing your environment and applications.

The [Wavefront Proxy](https://docs.wavefront.com/proxies.html) is a light-weight Java application that you send your metrics, histograms, and trace data to. It handles batching and transmission of your data to the Wavefront service in a secure, fast, and reliable manner.

## Requirements
  * Java 8 or higher
  * Maven

## Overview
  * pkg: Build and runtime packaging for the Wavefront proxy.
  * proxy: [Wavefront Proxy](https://docs.wavefront.com/proxies.html) source code.

  Please refer to the [project page](https://github.com/wavefrontHQ/wavefront-proxy/tree/master/proxy) for further details.

## To start developing

```
$ git clone https://github.com/wavefronthq/wavefront-proxy
$ cd wavefront-proxy
$ mvn clean install -DskipTests
```

## Contributing
Public contributions are always welcome. Please feel free to report issues or submit pull requests.
