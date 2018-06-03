# Dropwizard Metrics for Wavefront

Wavefront maintains reporter plugins for Dropwizard Metrics. Source code and more information about each reporter
can be found within these subdirectories:

- [Dropwizard Metrics](/dropwizard-metrics/dropwizard-metrics)
- [Dropwizard](/dropwizard-metrics/dropwizard-metrics-wavefront)

The Dropwizard Metrics library is intended for sending metrics from Dropwizard applications that are *not* running 
within the Dropwizard Microservice framework. The Dropwizard library, on the other hand, is designed to be linked into 
Dropwizard applications that then allows them to be configured with a Wavefront metrics reporter in their YAML 
configuration.