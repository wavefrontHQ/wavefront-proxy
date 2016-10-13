package com.wavefront.agent.config;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.exception.GrokException;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class LogsIngestionConfig extends Configuration {
  @JsonProperty
  public Integer aggregationIntervalSeconds = 5;
  @JsonProperty
  public List<MetricMatcher> counters = ImmutableList.of();
  @JsonProperty
  public List<MetricMatcher> gauges = ImmutableList.of();
  @JsonProperty
  public List<MetricMatcher> histograms = ImmutableList.of();
  @JsonProperty
  public List<String> additionalPatterns = ImmutableList.of();

  private String patternsFile = null;
  private Object patternsFileLock = new Object();

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

  // HACK: this must be called for the additional patterns feature to work.
  @Override
  public void verify() throws ConfigurationException {
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
      p.verify();
    }
    for (MetricMatcher p : gauges) {
      p.setPatternsFile(patternsFile());
      p.verify();
      ensure(p.hasCapture(p.getValueLabel()),
          "Must have a capture with label '" + p.getValueLabel() + "' for this gauge.");
    }
    for (MetricMatcher p : histograms) {
      p.setPatternsFile(patternsFile());
      p.verify();
      ensure(p.hasCapture(p.getValueLabel()),
          "Must have a capture with label '" + p.getValueLabel() + "' for this histogram.");
    }

  }
}
