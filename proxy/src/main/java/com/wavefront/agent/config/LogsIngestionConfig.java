package com.wavefront.agent.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.agent.logsharvesting.FlushProcessor;
import com.wavefront.agent.logsharvesting.FlushProcessorContext;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Top level configuration object for ingesting log data into the Wavefront Proxy. To turn on logs ingestion,
 * specify 'filebeatPort' and 'logsIngestionConfigFile' in the Wavefront Proxy Config File (typically
 * /etc/wavefront/wavefront-proxy/wavefront.conf, or /opt/wavefront/wavefront-proxy/conf/wavefront.conf).
 *
 * Every file with annotated with {@link JsonProperty} is parsed directly from your logsIngestionConfigFile, which is
 * YAML. Below is a sample config file which shows the features of direct logs ingestion. The "counters" section
 * corresponds to {@link #counters}, likewise for {@link #gauges} and {@link #histograms}. In each of these three
 * groups, the pricipal entry is a {@link MetricMatcher}. See the patterns file
 * <a href="https://github.com/wavefrontHQ/java/blob/master/proxy/src/main/resources/patterns/patterns">here</a> for
 * help defining patterns, also various grok debug tools (e.g. <a href="https://grokdebug.herokuapp.com/">this one</a>,
 * or use google)
 *
 * All metrics support dynamic naming with %{}. To see exactly what data we send as part of histograms, see
 * {@link FlushProcessor#processHistogram(MetricName, Histogram, FlushProcessorContext)}.
 *
 * <pre>
 * <code>
 * counters:
 *   # For log line "alpha 42", increment counterWithValue by 42
 *   - pattern: "alpha %{NUMBER:value}"
 *     metricName: "counterWithValue"
 *   # For log line "bravo", increment plainCounter by 1
 *   - pattern: "bravo"
 *     metricName: "plainCounter"
 *   # For log line "found item in myhost-123", increment "dynamic_myhost_123" by 1
 *   - pattern: "found item in %{WORD:name}-%{NUMBER:type}"
 *     metricName: "dynamic_%{name}_%{type}"
 *   # For log line "she sold 42 sea shells", increment "shells" by 42. See MYPATTERN below.
 *   - pattern: "%{MYPATTERN}"
 *     metricName: "shells"
 *     valueLabel: "value"
 *   # For log line "operation foo took 42 seconds in DC=oregon and AZ=2a", increment "foo.totalSeconds" with point
 *   # tags "theDC=oregon theAZ=az-2a" by 42
 *   - pattern: "operation %{WORD:op} took %{NUMBER:value} seconds in DC=%{WORD:dc}.*AZ=%{WORD:az}"
 *     metricName: "%{op}.totalSeconds"
 *     tagKeys:
 *       - "theDC"
 *       - "theAZ"
 *     tagValues:
 *       - "%{dc}"
 *       - "az-%{az}"
 *   # For log line '127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08 [en] (Win98; I ;Nav)"',
 *   # increment apacheBytes by 2326. See the patterns file.
 *   - pattern: "%{COMBINEDAPACHELOG}"
 *     metricName: "apacheBytes"
 *     valueLabel: "bytes"
 *
 * gauges:
 *   # For log line "temperature 78", set the "temp" metric to 78
 *   - pattern: "temperature %{NUMBER:value}"
 *     metricName: "temp"
 *   # For log line: "WARNING: [2878] (SUMMARY): points attempted: 859432; blocked: 0", set wavefrontPointsSent.2878 to 859432
 *   - pattern: '%{LOGLEVEL}: \[%{NUMBER:port}\] %{GREEDYDATA} points attempted: %{NUMBER:pointsAttempted}'
 *     metricName: "wavefrontPointsSent.%{port}"
 *     valueLabel: "pointsAttempted"
 *
 * histograms:
 *   # For log line "histo 123.45", add 123.45 to the running distribution. Will send metrics like p99, mean, ...
 *   - pattern: "histo %{NUMBER:value}"
 *     metricName: "myHisto"
 *
 * additionalPatterns:
 *   - "MYPATTERN she sold %{NUMBER:value} sea shells"
 * </code>
 * </pre>
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */

@SuppressWarnings("CanBeFinal")
public class LogsIngestionConfig extends Configuration {
  /**
   * How often metrics are aggregated and sent to wavefront. Histograms are cleared every time they are sent,
   * counters and gauges are not.
   */
  @JsonProperty
  public Integer aggregationIntervalSeconds = 60;

  /**
   * Counters to ingest from incoming log data.
   */
  @JsonProperty
  public List<MetricMatcher> counters = ImmutableList.of();

  /**
   * Gauges to ingest from incoming log data.
   */
  @JsonProperty
  public List<MetricMatcher> gauges = ImmutableList.of();

  /**
   * Histograms to ingest from incoming log data.
   */
  @JsonProperty
  public List<MetricMatcher> histograms = ImmutableList.of();

  /**
   * Additional grok patterns to use in pattern matching for the above {@link MetricMatcher}s.
   */
  @JsonProperty
  public List<String> additionalPatterns = ImmutableList.of();

  /**
   * Metrics are cleared from memory (and so their aggregation state is lost) if a metric is not
   * updated within this many milliseconds. Applicable only if useDeltaCounters = false.
   * Default: 3600000 (1 hour).
   */
  @JsonProperty
  public long expiryMillis = TimeUnit.HOURS.toMillis(1);

  /**
   * If true, use {@link com.yammer.metrics.core.WavefrontHistogram}s rather than {@link
   * com.yammer.metrics.core.Histogram}s. Histogram ingestion must be enabled on wavefront to use this feature. When
   * using Yammer histograms, the data is exploded into constituent metrics. See {@link
   * FlushProcessor#processHistogram(MetricName, Histogram, FlushProcessorContext)}.
   */
  @JsonProperty
  public boolean useWavefrontHistograms = false;

  /**
   * If true (default), simulate Yammer histogram behavior (report all stats as zeroes when histogram is empty).
   * Otherwise, only .count is reported with a zero value.
   */
  @JsonProperty
  public boolean reportEmptyHistogramStats = true;

  /**
   * If true, use delta counters instead of regular counters to prevent metric collisions when
   * multiple proxies are behind a load balancer. Default: true
   */
  @JsonProperty
  public boolean useDeltaCounters = true;

  /**
   * How often to check this config file for updates.
   */
  @JsonProperty
  public int configReloadIntervalSeconds = 5;

  @Override
  public void verifyAndInit() throws ConfigurationException {
    Map<String, String> additionalPatternMap = Maps.newHashMap();
    for (String pattern : additionalPatterns) {
      String[] parts = pattern.split(" ");
      String name = parts[0];
      String regex = String.join(" ", Arrays.copyOfRange(parts, 1, parts.length));
      additionalPatternMap.put(name, regex);
    }
    ensure(aggregationIntervalSeconds > 0, "aggregationIntervalSeconds must be positive.");
    for (MetricMatcher p : counters) {
      p.setAdditionalPatterns(additionalPatternMap);
      p.verifyAndInit();
    }
    for (MetricMatcher p : gauges) {
      p.setAdditionalPatterns(additionalPatternMap);
      p.verifyAndInit();
      ensure(p.hasCapture(p.getValueLabel()),
          "Must have a capture with label '" + p.getValueLabel() + "' for this gauge.");
    }
    for (MetricMatcher p : histograms) {
      p.setAdditionalPatterns(additionalPatternMap);
      p.verifyAndInit();
      ensure(p.hasCapture(p.getValueLabel()),
          "Must have a capture with label '" + p.getValueLabel() + "' for this histogram.");
    }
  }
}
