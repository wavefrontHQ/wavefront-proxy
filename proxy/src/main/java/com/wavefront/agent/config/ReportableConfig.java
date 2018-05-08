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
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * Wrapper class to simplify access to .properties file + track values as metrics as they are retrieved
 *
 * @author vasily@wavefront.com
 */
public class ReportableConfig {
  private static final Logger logger = Logger.getLogger(ReportableConfig.class.getCanonicalName());

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
    return getNumber(key, defaultValue, null, null);
  }

  public Number getNumber(String key, @Nullable Number defaultValue, @Nullable Number clampMinValue,
                          @Nullable Number clampMaxValue) {
    String property = prop.getProperty(key);
    if (property == null && defaultValue == null) return null;
    Long l;
    try {
      l = property == null ? defaultValue.longValue() : Long.parseLong(property.trim());
    } catch (NumberFormatException e) {
      throw new NumberFormatException("Config setting \"" + key + "\": invalid number format \"" + property + "\"");
    }
    if (clampMinValue != null && l < clampMinValue.longValue()) {
      logger.log(Level.WARNING, key + " (" + l + ") is less than " + clampMinValue +
          ", will default to " + clampMinValue);
      reportGauge(clampMinValue, new MetricName("config", "", key));
      return clampMinValue;
    }
    if (clampMaxValue != null && l > clampMaxValue.longValue()) {
      logger.log(Level.WARNING, key + " (" + l + ") is greater than " + clampMaxValue +
          ", will default to " + clampMaxValue);
      reportGauge(clampMaxValue, new MetricName("config", "", key));
      return clampMaxValue;
    }
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

  public Boolean getBoolean(String key, Boolean defaultValue) {
    Boolean b = Boolean.parseBoolean(prop.getProperty(key, String.valueOf(defaultValue)).trim());
    reportGauge(b ? 1 : 0, new MetricName("config", "", key));
    return b;
  }

  public Boolean isDefined(String key) {
    return prop.getProperty(key) != null;
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
