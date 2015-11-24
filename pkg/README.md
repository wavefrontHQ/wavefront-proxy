Wavefront Proxy Packaging
=========================

Tools
-----
* Install [fpm](https://github.com/jordansissel/fpm) with Ruby's GEM. This is
  for building linux packages, which are used to deploy the package to
  customers' machines and to automatically manage updates.
* Install [packr](https://github.com/libgdx/packr), which can be downloaded
  directly as a JAR from the github page. This is for producing a hermetic
  proxy binary, which is written in java and backed by a JRE we verify.

Methodology
-----------
Wrap packr.jar in a shell script called `packr` and place it on your path.

    #!/bin/bash
    java -jar `dirname $0`/packr.jar $@

Then run build.sh, e.g.

    ./build.sh ~/jres/openjdk-1.7.0-u80-unofficial-linux-amd64-installer.zip wavefront-proxy-3.1.jar deb 3.1 4
