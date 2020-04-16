package com.wavefront.agent.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.agent.logsharvesting.LogsMessage;
import com.wavefront.data.Validation;

import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;
import wavefront.report.TimeSeries;

/**
 * Object defining transformation between a log line into structured telemetry data.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
@SuppressWarnings("CanBeFinal")
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
   * Override the host name for the point we're creating from the current log line. May contain
   * substitutions from {@link #pattern}, similar to metricName.
   */
  @JsonProperty
  private String hostName = "";
  /**
   * A list of tags for the point you are creating from the logLine. If you don't want any tags, leave empty. For
   * example, could be ["myDatacenter", "myEnvironment"] Also see {@link #tagValues}.
   */
  @JsonProperty
  private List<String> tagKeys = ImmutableList.of();
  /**
   * Deprecated, use tagValues instead
   *
   * A parallel array to {@link #tagKeys}. Each entry is a label you defined in {@link #pattern}. For example, you
   * might use ["datacenter", "env"] if your pattern is
   * "operation foo in %{WORD:datacenter}:%{WORD:env} succeeded in %{NUMBER:value} milliseconds", and your log line is
   * "operation foo in 2a:prod succeeded in 1234 milliseconds", then you would generate the point
   * "foo.latency 1234 myDataCenter=2a myEnvironment=prod"
   */
  @Deprecated
  @JsonProperty
  private List<String> tagValueLabels = ImmutableList.of();
  /**
   * A parallel array to {@link #tagKeys}. Each entry is a string value that will be used as a tag value,
   * substituting %{...} placeholders with corresponding labels you defined in {@link #pattern}. For example, you
   * might use ["%{datacenter}", "%{env}-environment", "staticTag"] if your pattern is
   * "operation foo in %{WORD:datacenter}:%{WORD:env} succeeded in %{NUMBER:value} milliseconds", and your log line is
   * "operation foo in 2a:prod succeeded in 1234 milliseconds", then you would generate the point
   * "foo.latency 1234 myDataCenter=2a myEnvironment=prod-environment myStaticValue=staticTag"
   */
  @JsonProperty
  private List<String> tagValues = ImmutableList.of();
  /**
   * The label which is used to parse a telemetry datum from the log line.
   */
  @JsonProperty
  private String valueLabel = "value";
  private Grok grok = null;
  private Map<String, String> additionalPatterns = Maps.newHashMap();

  public String getValueLabel() {
    return valueLabel;
  }

  public String getPattern() {
    return pattern;
  }

  public void setAdditionalPatterns(Map<String, String> additionalPatterns) {
    this.additionalPatterns = additionalPatterns;
  }

  // Singleton grok for this pattern.
  private Grok grok() {
    if (grok != null) return grok;
    synchronized (grokLock) {
      if (grok != null) return grok;
      try {
        grok = new Grok();
        InputStream patternStream = getClass().getClassLoader().
            getResourceAsStream("patterns/patterns");
        if (patternStream != null) {
          grok.addPatternFromReader(new InputStreamReader(patternStream));
        }
        additionalPatterns.forEach((key, value) -> {
          try {
            grok.addPattern(key, value);
          } catch (GrokException e) {
            logger.severe("Invalid grok pattern: " + pattern);
            throw new RuntimeException(e);
          }
        });
        grok.compile(pattern);
      } catch (GrokException e) {
        logger.severe("Invalid grok pattern: " + pattern);
        throw new RuntimeException(e);
      }
      return grok;
    }
  }

  private static String expandTemplate(String template, Map<String, Object> replacements) {
    if (template.contains("%{")) {
      StringBuffer result = new StringBuffer();
      Matcher placeholders = Pattern.compile("%\\{(.*?)}").matcher(template);
      while (placeholders.find()) {
        if (placeholders.group(1).isEmpty()) {
          placeholders.appendReplacement(result, placeholders.group(0));
        } else {
          if (replacements.get(placeholders.group(1)) != null) {
            placeholders.appendReplacement(result, (String)replacements.get(placeholders.group(1)));
          } else {
            placeholders.appendReplacement(result, placeholders.group(0));
          }
        }
      }
      placeholders.appendTail(result);
      return result.toString();
    }
    return template;
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
    Map<String, Object> matches = match.toMap();
    if (output != null) {
      if (matches.containsKey(valueLabel)) {
        output[0] = Double.parseDouble((String) matches.get(valueLabel));
      } else {
        output[0] = null;
      }
    }
    TimeSeries.Builder builder = TimeSeries.newBuilder();
    String dynamicName = expandTemplate(metricName, matches);
    String sourceName = StringUtils.isBlank(hostName) ?
        logsMessage.hostOrDefault("parsed-logs") :
        expandTemplate(hostName, matches);
    // Important to use a tree map for tags, since we need a stable ordering for the serialization
    // into the LogsIngester.metricsCache.
    Map<String, String> tags = Maps.newTreeMap();
    for (int i = 0; i < tagKeys.size(); i++) {
      String tagKey = tagKeys.get(i);
      if (tagValues.size() > 0) {
        String value = expandTemplate(tagValues.get(i), matches);
        if(StringUtils.isNotBlank(value)) {
          tags.put(tagKey, value);
        }
      } else {
        String tagValueLabel = tagValueLabels.get(i);
        if (!matches.containsKey(tagValueLabel)) {
          // What happened? We shouldn't have had matchEnd != 0 above...
          logger.severe("Application error: unparsed tag key.");
          continue;
        }
        String value = (String) matches.get(tagValueLabel);
        if(StringUtils.isNotBlank(value)) {
          tags.put(tagKey, value);
        }
      }
    }
    builder.setAnnotations(tags);
    return builder.setMetric(dynamicName).setHost(sourceName).build();
  }

  public boolean hasCapture(String label) {
    return grok().getNamedRegexCollection().containsValue(label);
  }

  @Override
  public void verifyAndInit() throws ConfigurationException {
    ensure(StringUtils.isNotBlank(pattern), "pattern must not be empty.");
    ensure(StringUtils.isNotBlank(metricName), "metric name must not be empty.");
    String fauxMetricName = metricName.replaceAll("%\\{.*\\}", "");
    ensure(Validation.charactersAreValid(fauxMetricName), "Metric name has illegal characters: " + metricName);
    ensure(!(tagValues.size() > 0 && tagValueLabels.size() > 0), "tagValues and tagValueLabels can't be used together");
    ensure(tagKeys.size() == Math.max(tagValueLabels.size(), tagValues.size()),
        "tagKeys and tagValues/tagValueLabels must be parallel arrays.");
  }
}
