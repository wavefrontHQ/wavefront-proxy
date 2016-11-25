package com.wavefront.agent.config;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.agent.Validation;
import com.wavefront.agent.logsharvesting.LogsMessage;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.annotation.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;
import sunnylabs.report.TimeSeries;

/**
 * Object defining transformation between a log line into structured telemetry data.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
@ThreadSafe
public class MetricMatcher extends Configuration {
  protected static final Logger logger = Logger.getLogger(MetricMatcher.class.getCanonicalName());
  private final Object grokLock = new Object();
  /**
   * A Logstash style grok pattern, see
   * https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html and http://grokdebug.herokuapp.com/.
   * If a log line matches this pattern, that log line will be transformed into a metric per the other fields
   * in this object.
   */
  @JsonProperty
  private String pattern = "";
  /**
   * The metric name for the point we're creating from the current log line. may contain substitutions from
   * {@link #pattern}. For example, if your pattern is "operation %{WORD:opName} ...",
   * and your log line is "operation baz ..." then you can use the metric name "operations.%{opName}".
   */
  @JsonProperty
  private String metricName = "";
  /**
   * A list of tags for the point you are creating from the logLine. If you don't want any tags, leave empty. For
   * example, could be ["myDatacenter", "myEnvironment"] Also see {@link #tagValueLabels}.
   */
  @JsonProperty
  private List<String> tagKeys = ImmutableList.of();
  /**
   * A parallel array to {@link #tagKeys}. Each entry is a label you defined in {@link #pattern}. For example, you
   * might use ["datacenter", "env"] if your pattern is
   * "operation foo in %{WORD:datacenter}:%{WORD:env} succeeded in %{NUMBER:value} milliseconds", and your log line is
   * "operation foo in 2a:prod succeeded in 1234 milliseconds", then you would generate the point
   * "foo.latency 1234 myDataCenter=2a myEnvironment=prod"
   */
  @JsonProperty
  private List<String> tagValueLabels = ImmutableList.of();
  /**
   * The label which is used to parse a telemetry datum from the log line.
   */
  @JsonProperty
  private String valueLabel = "value";
  private Grok grok = null;

  public String getValueLabel() {
    return valueLabel;
  }

  public String getPattern() {
    return pattern;
  }

  private String patternsFile = null;

  public void setPatternsFile(String patternsFile) {
    this.patternsFile = patternsFile;
  }

  // Singleton grok for this pattern.
  private Grok grok() {
    if (grok != null) return grok;
    synchronized (grokLock) {
      if (grok != null) return grok;
      try {
        grok = Grok.create(patternsFile);
        grok.compile(pattern);
      } catch (GrokException e) {
        logger.severe("Invalid grok pattern: " + pattern);
        throw Throwables.propagate(e);
      }
      return grok;
    }
  }

  private String dynamicMetricName(Map<String, Object> replacements) {
    if (!metricName.contains("%{")) return metricName;
    String dynamicName = metricName;
    for (String key : replacements.keySet()) {
      String value = (String) replacements.get(key);
      if (value == null) continue;
      dynamicName = StringUtils.replace(dynamicName, "%{" + key + "}", value);
    }
    return dynamicName;
  }

  /**
   * Convert the given message to a timeSeries and a telemetry datum.
   *
   * @param logsMessage     The message to convert.
   * @param output          The telemetry parsed from the filebeat message.
   */
  public TimeSeries timeSeries(LogsMessage logsMessage, Double[] output) throws NumberFormatException {
    Match match = grok().match(logsMessage.getLogLine());
    match.captures();
    if (match.getEnd() == 0) return null;
    if (output != null) {
      if (match.toMap().containsKey(valueLabel)) {
        output[0] = Double.parseDouble((String) match.toMap().get(valueLabel));
      } else {
        output[0] = null;
      }
    }
    TimeSeries.Builder builder = TimeSeries.newBuilder();
    String dynamicName = dynamicMetricName(match.toMap());
    // Important to use a tree map for tags, since we need a stable ordering for the serialization
    // into the LogsIngester.metricsCache.
    Map<String, String> tags = Maps.newTreeMap();
    for (int i = 0; i < tagKeys.size(); i++) {
      String tagKey = tagKeys.get(i);
      String tagValueLabel = tagValueLabels.get(i);
      if (!match.toMap().containsKey(tagValueLabel)) {
        // What happened? We shouldn't have had matchEnd != 0 above...
        logger.severe("Application error: unparsed tag key.");
        continue;
      }
      String value = (String) match.toMap().get(tagValueLabel);
      tags.put(tagKey, value);
    }
    builder.setAnnotations(tags);
    return builder.setMetric(dynamicName).setHost(logsMessage.hostOrDefault("parsed-logs")).build();
  }

  public boolean hasCapture(String label) {
    return grok().getNamedRegexCollection().values().contains(label);
  }


  @Override
  public void verifyAndInit() throws ConfigurationException {
    ensure(StringUtils.isNotBlank(pattern), "pattern must not be empty.");
    ensure(StringUtils.isNotBlank(metricName), "metric name must not be empty.");
    String fauxMetricName = metricName.replaceAll("%\\{.*\\}", "");
    ensure(Validation.charactersAreValid(fauxMetricName), "Metric name has illegal characters: " + metricName);
    ensure(tagKeys.size() == tagValueLabels.size(), "tagKeys and tagValueLabels must be parallel arrays.");
  }

}
