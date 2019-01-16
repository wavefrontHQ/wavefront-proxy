package com.wavefront.integrations.metrics;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.hibernate.validator.constraints.NotEmpty;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;

import javax.validation.constraints.NotNull;

import io.dropwizard.metrics.BaseReporterFactory;
import io.dropwizard.validation.PortRange;

/**
 * A factory for {@link WavefrontReporter} instances.
 * <p/>
 * <b>Configuration Parameters:</b>
 * <table>
 *     <tr>
 *         <td>Name</td>
 *         <td>Default</td>
 *         <td>Description</td>
 *     </tr>
 *     <tr>
 *         <td>proxyHost</td>
 *         <td>localhost</td>
 *         <td>The hostname of the Wavefront proxy to report to.</td>
 *     </tr>
 *     <tr>
 *         <td>proxyPort</td>
 *         <td>2003</td>
 *         <td>The port of the Wavefront proxy to report to.</td>
 *     </tr>
 *     <tr>
 *         <td>prefix</td>
 *         <td><i>None</i></td>
 *         <td>The prefix for Metric key names to report to Wavefront.</td>
 *     </tr>
 *     <tr>
 *         <td>metricSource</td>
 *         <td><i>canonical name of localhost</i></td>
 *         <td>This is the source name all metrics will report to Wavefront under.</td>
 *     </tr>
 *     <tr>
 *         <td>pointTags</td>
 *         <td><i>None</i></td>
 *         <td>Key-value pairs for point tags to be added to each point reporting to Wavefront.</td>
 *     </tr>
 * </table>
 */
@JsonTypeName("wavefront")
public class WavefrontReporterFactory extends BaseReporterFactory {
  @NotEmpty
  private String proxyHost = "localhost";

  @PortRange
  private int proxyPort = 2878;

  @NotNull
  private String prefix = "";

  @NotNull
  private String metricSource = "";

  @NotNull
  private Map<String, String> pointTags = Collections.emptyMap();

  @JsonProperty
  public String getProxyHost() {
    return proxyHost;
  }

  @JsonProperty
  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  @JsonProperty
  public int getProxyPort() {
    return proxyPort;
  }

  @JsonProperty
  public void setProxyPort(int proxyPort) {
    this.proxyPort = proxyPort;
  }

  @JsonProperty
  public String getPrefix() {
    return prefix;
  }

  @JsonProperty
  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  @JsonProperty
  public String getMetricSource() {
    return metricSource;
  }

  @JsonProperty
  public void setMetricSource(String metricSource) {
    this.metricSource = metricSource;
  }

  @JsonProperty("pointTags")
  public Map<String, String> getPointTags() {
    return pointTags;
  }

  @JsonProperty("pointTags")
  public void setPointTags(Map<String, String> pointTags) {
    this.pointTags = ImmutableMap.copyOf(pointTags);
  }


  @Override
  public ScheduledReporter build(MetricRegistry registry) {
    return WavefrontReporter.forRegistry(registry)
        .convertDurationsTo(getDurationUnit())
        .convertRatesTo(getRateUnit())
        .filter(getFilter())
        .prefixedWith(getPrefix())
        .withSource(getSource())
        .disabledMetricAttributes(getDisabledAttributes())
        .withPointTags(pointTags)
        .build(proxyHost, proxyPort);
  }

  private String getSource() {
    try {
      return Strings.isNullOrEmpty(getMetricSource()) ?
          InetAddress.getLocalHost().getCanonicalHostName() :
          getMetricSource();
    } catch (UnknownHostException ex) {
      return "localhost";
    }
  }
}
