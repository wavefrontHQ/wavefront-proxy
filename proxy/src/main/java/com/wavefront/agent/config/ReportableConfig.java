package com.wavefront.agent.config;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class to simplify access to .properties file + track values as metrics as they are
 * retrieved
 */
public class ReportableConfig extends Properties {
  private static final Logger logger =
      LoggerFactory.getLogger(ReportableConfig.class.getCanonicalName());

  public ReportableConfig(String fileName) throws IOException {
    this.load(Files.newInputStream(Paths.get(fileName)));
  }

  public ReportableConfig() {}

  /** Returns string value for the property without tracking it as a metric */
  public String getRawProperty(String key, String defaultValue) {
    return this.getProperty(key, defaultValue);
  }

  public int getInteger(String key, Number defaultValue) {
    return getNumber(key, defaultValue).intValue();
  }

  public long getLong(String key, Number defaultValue) {
    return getNumber(key, defaultValue).longValue();
  }

  public double getDouble(String key, Number defaultValue) {
    return getNumber(key, defaultValue).doubleValue();
  }

  public Number getNumber(String key, Number defaultValue) {
    return getNumber(key, defaultValue, null, null);
  }

  public Number getNumber(
      String key,
      @Nullable Number defaultValue,
      @Nullable Number clampMinValue,
      @Nullable Number clampMaxValue) {
    String property = this.getProperty(key);
    if (property == null && defaultValue == null) return null;
    double d;
    try {
      d = property == null ? defaultValue.doubleValue() : Double.parseDouble(property.trim());
    } catch (NumberFormatException e) {
      throw new NumberFormatException(
          "Config setting \"" + key + "\": invalid number format \"" + property + "\"");
    }
    if (clampMinValue != null && d < clampMinValue.longValue()) {
      logger.warn(
          key
              + " ("
              + d
              + ") is less than "
              + clampMinValue
              + ", will default to "
              + clampMinValue);
      //      reportGauge(clampMinValue, new MetricName("config", "", key));
      return clampMinValue;
    }
    if (clampMaxValue != null && d > clampMaxValue.longValue()) {
      logger.warn(
          key
              + " ("
              + d
              + ") is greater than "
              + clampMaxValue
              + ", will default to "
              + clampMaxValue);
      //      reportGauge(clampMaxValue, new MetricName("config", "", key));
      return clampMaxValue;
    }
    //    reportGauge(d, new MetricName("config", "", key));
    return d;
  }

  public String getString(String key, String defaultValue) {
    return getString(key, defaultValue, null);
  }

  public String getString(
      String key, String defaultValue, @Nullable Function<String, String> converter) {
    String s = this.getProperty(key, defaultValue);
    //    if (s == null || s.trim().isEmpty()) {
    //      reportGauge(0, new MetricName("config", "", key));
    //    } else {
    //      reportGauge(
    //          1,
    //          new TaggedMetricName("config", key, "value", converter == null ? s :
    // converter.apply(s)));
    //    }
    return s;
  }

  public Boolean getBoolean(String key, Boolean defaultValue) {
    Boolean b = Boolean.parseBoolean(this.getProperty(key, String.valueOf(defaultValue)).trim());
    //    reportGauge(b ? 1 : 0, new MetricName("config", "", key));
    return b;
  }

  public Boolean isDefined(String key) {
    return this.getProperty(key) != null;
  }

  public static void reportSettingAsGauge(Supplier<Number> numberSupplier, String key) {
    reportGauge(numberSupplier, new MetricName("config", "", key));
  }

  public static void reportGauge(Supplier<Number> numberSupplier, MetricName metricName) {
    Metrics.newGauge(
        metricName,
        new Gauge<Double>() {
          @Override
          public Double value() {
            return numberSupplier.get().doubleValue();
          }
        });
  }

  public static void reportGauge(Number number, MetricName metricName) {
    Metrics.newGauge(
        metricName,
        new Gauge<Double>() {
          @Override
          public Double value() {
            return number.doubleValue();
          }
        });
  }
}
