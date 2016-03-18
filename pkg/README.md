Wavefront Proxy Packaging
=========================

Tools
-----
* Install [fpm](https://github.com/jordansissel/fpm) with Ruby's GEM. This is
  for building linux packages, which are used to deploy the package to
  customers' machines and to automatically manage updates.
* Install [maven](https://maven.apache.org/index.html).

Methodology
-----------
Run build.sh, e.g.

    ./build.sh ~/jres/openjdk-1.7.0-u80 deb 3.1 4

This will build the agent from head and package it into a deb or an rpm. The agent will use whatever JRE
you bundle with it -- Wavefront uses Zulu 8.11.0.1 a8c3eea6250f.
