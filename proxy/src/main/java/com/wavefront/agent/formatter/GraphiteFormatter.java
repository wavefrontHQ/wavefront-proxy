package com.wavefront.agent.formatter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import com.wavefront.common.MetricMangler;

/**
 * Specific formatter for the graphite/collectd world of metric-munged names.
 *
 * @author dev@wavefront.com.
 */
public class GraphiteFormatter implements Function<String, String> {

  private final MetricMangler metricMangler;
  private final AtomicLong ops = new AtomicLong();

  public GraphiteFormatter(String strFields, String strDelimiters, String strFieldsToRemove) {
    Preconditions.checkNotNull(strFields, "strFields must be defined");
    Preconditions.checkNotNull(strDelimiters, "strFields must be defined");
    metricMangler = new MetricMangler(strFields, strDelimiters, strFieldsToRemove);
  }

  public long getOps() {
    return ops.get();
  }

  public MetricMangler getMetricMangler() {
    return metricMangler;
  }

  @Override
  public String apply(String mesg) {
    // Resting place for output
    StringBuffer finalMesg = new StringBuffer();

    // 1. Extract fields
    String[] regions = mesg.trim().split(" ");
    final MetricMangler.MetricComponents components = metricMangler.extractComponents(regions[0]);
    
    finalMesg.append(components.metric);
    finalMesg.append(" ");

    // 2. Add back value, timestamp if existent, etc. We don't assume you have point tags
    //  because your first tag BETTER BE A HOST, BUDDY.
    for (int index = 1; index < regions.length; index++) {
      finalMesg.append(regions[index]);
      finalMesg.append(" ");
    }

    // 3. Loop over host components in configured order, and replace all delimiters with dots
    finalMesg.append("source=");
    finalMesg.append(components.source);

    ops.incrementAndGet();

    return finalMesg.toString();
  }
}
