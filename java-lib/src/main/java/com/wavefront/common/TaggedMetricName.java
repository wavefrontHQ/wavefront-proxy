package com.wavefront.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.MetricName;

import javax.management.ObjectName;
import java.util.Collections;
import java.util.Map;

/**
 * A taggable metric name.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class TaggedMetricName extends MetricName {

  private final Map<String, String> tags;

  /**
   * A simple metric that would be concatenated when reported, e.g. "jvm", "name" would become jvm.name.
   *
   * @param group Prefix of the metric.
   * @param name  The name of the metric.
   */
  public TaggedMetricName(String group, String name) {
    super(group, "", name);
    tags = Collections.emptyMap();
  }

  public TaggedMetricName(String group, String name, String... tagAndValues) {
    this(group, name, makeTags(tagAndValues));
  }

  public TaggedMetricName(String group, String name, Map<String, String> tags) {
    this(group, name, makeTags(tags));
  }

  public TaggedMetricName(String group, String name, Pair<String, String>... tags) {
    super(group, "", name, null, createMBeanName(group, "", name, tags));
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Pair<String, String> tag : tags) {
      if (tag != null && tag._1 != null && tag._2 != null) {
        builder.put(tag._1, tag._2);
      }
    }
    this.tags = builder.build();
  }

  public Map<String, String> getTags() {
    return tags;
  }

  private static Pair<String, String>[] makeTags(Map<String, String> tags) {
    Pair<String, String>[] toReturn = new Pair[tags.size()];
    int i = 0;
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      toReturn[i] = new Pair<String, String>(entry.getKey(), entry.getValue());
      i++;
    }
    return toReturn;
  }

  private static Pair<String, String>[] makeTags(String... tagAndValues) {
    Preconditions.checkArgument((tagAndValues.length & 1) == 0, "must have even number of tag values");
    Pair<String, String>[] toReturn = new Pair[tagAndValues.length / 2];
    for (int i = 0; i < tagAndValues.length; i += 2) {
      String tag = tagAndValues[i];
      String value = tagAndValues[i + 1];
      if (tag != null && value != null) {
        toReturn[i / 2] = new Pair<String, String>(tag, value);
      }
    }
    return toReturn;
  }

  private static String createMBeanName(String group, String type, String name, Pair<String, String>... tags) {
    final StringBuilder nameBuilder = new StringBuilder();
    nameBuilder.append(ObjectName.quote(group));
    nameBuilder.append(":type=");
    nameBuilder.append(ObjectName.quote(type));
    if (name.length() > 0) {
      nameBuilder.append(",name=");
      nameBuilder.append(ObjectName.quote(name));
    }
    for (Pair<String, String> tag : tags) {
      if (tag != null) {
        nameBuilder.append(",");
        nameBuilder.append(tag._1);
        nameBuilder.append("=");
        nameBuilder.append(tag._2);
      }
    }
    return nameBuilder.toString();
  }
}
