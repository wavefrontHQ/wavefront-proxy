package com.wavefront;

import org.glassfish.jersey.server.ContainerRequest;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

import static com.wavefront.DropwizardAppMetricNameUtils.metricName;

/**
 * A filter to generate Wavefront metrics and histograms for Jersey API requests/responses
 *
 * @author Sushant Dewan (sushant@wavefront.com).
 */
public class WavefrontJerseyFilter implements ContainerRequestFilter, ContainerResponseFilter {

  private final DropwizardApplicationReporter wfAppReporter;
  private final ThreadLocal<Long> startTime = new ThreadLocal<>();

  public WavefrontJerseyFilter(DropwizardApplicationReporter wfAppReporter) {
    this.wfAppReporter = wfAppReporter;
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    if (containerRequestContext instanceof ContainerRequest) {
      ContainerRequest request = (ContainerRequest) containerRequestContext;
      startTime.set(System.currentTimeMillis());
      String metricName = metricName(request);
      if (metricName != null) {
        wfAppReporter.incrementCounter(metricName);
      }
    }
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext,
                     ContainerResponseContext containerResponseContext)
      throws IOException {
    if (containerRequestContext instanceof ContainerRequest) {
      ContainerRequest request = (ContainerRequest) containerRequestContext;
      String metricName = metricName(request, containerResponseContext);
      if (metricName != null) {
        wfAppReporter.incrementCounter(metricName);
        long apiLatency = System.currentTimeMillis() - startTime.get();
        wfAppReporter.updateHistogram(metricName + ".latency", apiLatency);
      }
    }
  }
}
