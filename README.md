# security advisories

Wavefront proxy version 10.10 and earlier is impacted by a Log4j vulnerability — CVE-2021-44228. VMware Security Advisory (VMSA): CVE-2021-44228 – VMSA-2021-0028 discusses this vulnerability and its impact on VMware products.
 
Patches
 
Wavefront proxy version 10.11 and later use a version of Log4j that addresses this vulnerability.

-----

# Wavefront Proxy Project [![Build Status](https://travis-ci.org/wavefrontHQ/wavefront-proxy.svg?branch=master)](https://travis-ci.org/wavefrontHQ/wavefront-proxy)

[Wavefront](https://docs.wavefront.com/) is a high-performance streaming analytics platform for monitoring and optimizing your environment and applications.

The [Wavefront Proxy](https://docs.wavefront.com/proxies.html) is a light-weight Java application that you send your metrics, histograms, and trace data to. It handles batching and transmission of your data to the Wavefront service in a secure, fast, and reliable manner.

## Requirements
  * Java 8, 9, 10 or 11 (11 recommended)
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
