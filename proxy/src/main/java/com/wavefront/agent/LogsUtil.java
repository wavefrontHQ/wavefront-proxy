package com.wavefront.agent;

import com.wavefront.agent.formatter.DataFormat;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Sumit Deo (deosu@vmware.com)
 */
public class LogsUtil {

  public static final Set<DataFormat> LOGS_DATA_FORMATS =
      new HashSet<>(
          Arrays.asList(
              DataFormat.LOGS_JSON_ARR,
              DataFormat.LOGS_JSON_LINES,
              DataFormat.LOGS_JSON_CLOUDWATCH));

  public static Counter getOrCreateLogsCounterFromRegistry(
      MetricsRegistry registry, DataFormat format, String group, String name) {
    return registry.newCounter(
        new TaggedMetricName(group, name, "format", format.name().toLowerCase()));
  }

  public static Histogram getOrCreateLogsHistogramFromRegistry(
      MetricsRegistry registry, DataFormat format, String group, String name) {
    return registry.newHistogram(
        new TaggedMetricName(group, name, "format", format.name().toLowerCase()), false);
  }
}
