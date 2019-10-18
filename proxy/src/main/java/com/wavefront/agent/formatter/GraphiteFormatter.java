package com.wavefront.agent.formatter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.wavefront.common.MetricMangler;

import java.util.concurrent.atomic.AtomicLong;

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
    StringBuilder finalMesg = new StringBuilder();

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

    // 3. Add source, if available
    if (components.source != null) {
      finalMesg.append("source=");
      finalMesg.append(components.source);
    }

    // 4. Add Graphite 1.1+ tags
    if (components.annotations != null) {
      for (int index = 0; index < components.annotations.length; index++) {
        finalMesg.append(" ");
        finalMesg.append(components.annotations[index]);
      }
    }

    ops.incrementAndGet();

    return finalMesg.toString();
  }
}
