package com.wavefront.agent.config;

import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.function.Function;

import javax.annotation.Nullable;

/**
 * Wrapper class to simplify access to .properties file + track values as metrics as they are retrieved
 *
 * @author vasily@wavefront.com
 */
public class ReportableConfig {
  private Properties prop = new Properties();

  public ReportableConfig(InputStream stream) throws IOException {
    prop.load(stream);
  }

  public ReportableConfig(String fileName) throws IOException {
    prop.load(new FileInputStream(fileName));
  }

  /**
   * Returns string value for the property without tracking it as a metric
   *
   */
  public String getRawProperty(String key, String defaultValue) {
    return prop.getProperty(key, defaultValue);
  }

  public Number getNumber(String key, Number defaultValue) {
    Long l = Long.parseLong(prop.getProperty(key, String.valueOf(defaultValue)).trim());
    reportGauge(l, new MetricName("config", "", key));
    return l;
  }

  public String getString(String key, String defaultValue) {
    return getString(key, defaultValue, null);
  }

  public String getString(String key, String defaultValue,
                          @Nullable Function<String, String> converter) {
    String s = prop.getProperty(key, defaultValue);
    if (s == null || s.trim().isEmpty()) {
      reportGauge(0, new MetricName("config", "", key));
    } else {
      reportGauge(1, new TaggedMetricName("config", key, "value", converter == null ? s : converter.apply(s)));
    }
    return s;
  }

  public Boolean getPropertyAsBoolean(String key, Boolean defaultValue) {
    Boolean b = Boolean.parseBoolean(prop.getProperty(key, String.valueOf(defaultValue)).trim());
    reportGauge(b ? 1 : 0, new MetricName("config", "", key));
    return b;
  }

  public void reportSettingAsGauge(Number number, String key) {
    reportGauge(number, new MetricName("config", "", key));
  }

  public void reportGauge(Number number, MetricName metricName) {
    Metrics.newGauge(metricName,
        new Gauge<Double>() {
          @Override
          public Double value() {
            return number.doubleValue();
          }
        }
    );
  }
}
