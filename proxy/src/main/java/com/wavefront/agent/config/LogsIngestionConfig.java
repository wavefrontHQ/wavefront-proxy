package com.wavefront.agent.config;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wavefront.agent.logsharvesting.FlushProcessorContext;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.exception.GrokException;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class LogsIngestionConfig extends Configuration {
  /**
   * How often metrics are aggregated and sent to wavefront. Histograms are cleared every time they are sent,
   * counters and gauges are not.
   */
  @JsonProperty
  public Integer aggregationIntervalSeconds = 5;
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
   * Metrics are cleared from memory (and so their aggregation state is lost) if a metric is not updated
   * within this many milliseconds.
   */
  @JsonProperty
  public long expiryMillis = TimeUnit.HOURS.toMillis(1);
  /**
   * If true, use {@link com.yammer.metrics.core.WavefrontHistogram}s rather than {@link
   * com.yammer.metrics.core.Histogram}s. Histogram ingestion must be enabled on wavefront to use this feature. When
   * using Yammer histograms, the data is exploded into constituent metrics. See {@link
   * com.wavefront.agent.logsharvesting.FlushProcessor#processHistogram(MetricName, Histogram, FlushProcessorContext)}.
   */
  @JsonProperty
  public boolean useWavefrontHistograms = false;

  private String patternsFile = null;
  private Object patternsFileLock = new Object();

  /**
   * @return The path to a temporary file (on disk) containing grok patterns, to be consumed by {@link Grok}.
   */
  public String patternsFile() {
    if (patternsFile != null) return patternsFile;
    synchronized (patternsFileLock) {
      if (patternsFile != null) return patternsFile;
      try {
        File temp = File.createTempFile("patterns", ".tmp");
        InputStream patternInputStream = getClass().getClassLoader().getResourceAsStream("patterns/patterns");
        FileOutputStream fileOutputStream = new FileOutputStream(temp);
        IOUtils.copy(patternInputStream, fileOutputStream);
        PrintWriter printWriter = new PrintWriter(fileOutputStream);
        for (String pattern : additionalPatterns) {
          printWriter.write("\n" + pattern);
        }
        printWriter.close();
        patternsFile = temp.getAbsolutePath();
        return patternsFile;
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public void verifyAndInit() throws ConfigurationException {
    Grok grok = new Grok();
    for (String pattern : additionalPatterns) {
      String[] parts = pattern.split(" ");
      String name = parts[0];
      String regex = String.join(" ", Arrays.copyOfRange(parts, 1, parts.length));
      try {
        grok.addPattern(name, regex);
      } catch (GrokException e) {
        throw new ConfigurationException("bad grok pattern: " + pattern);
      }
    }
    ensure(aggregationIntervalSeconds > 0, "aggregationIntervalSeconds must be positive.");
    for (MetricMatcher p : counters) {
      p.setPatternsFile(patternsFile());
      p.verifyAndInit();
    }
    for (MetricMatcher p : gauges) {
      p.setPatternsFile(patternsFile());
      p.verifyAndInit();
      ensure(p.hasCapture(p.getValueLabel()),
          "Must have a capture with label '" + p.getValueLabel() + "' for this gauge.");
    }
    for (MetricMatcher p : histograms) {
      p.setPatternsFile(patternsFile());
      p.verifyAndInit();
      ensure(p.hasCapture(p.getValueLabel()),
          "Must have a capture with label '" + p.getValueLabel() + "' for this histogram.");
    }
  }
}
