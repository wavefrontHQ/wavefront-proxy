package com.wavefront.agent.histogram;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Uniquely identifies a time-series - time-interval pair.
 * These are the base sample aggregation scopes on the agent.
 * Refactored from HistogramUtils.
 *
 * @author Tim Schmidt (tim@wavefront.com)
 * @author vasily@wavefront.com
 */
public class HistogramKey {
  // NOTE: fields are not final to allow object reuse
  private byte granularityOrdinal;
  private int binId;
  private String metric;
  @Nullable
  private String source;
  @Nullable
  private String[] tags;

  HistogramKey(byte granularityOrdinal, int binId, @Nonnull String metric,
               @Nullable String source, @Nullable String[] tags) {
    this.granularityOrdinal = granularityOrdinal;
    this.binId = binId;
    this.metric = metric;
    this.source = source;
    this.tags = ((tags == null || tags.length == 0) ? null : tags);
  }

  HistogramKey() {
  }

  public byte getGranularityOrdinal() {
    return granularityOrdinal;
  }

  public int getBinId() {
    return binId;
  }

  public String getMetric() {
    return metric;
  }

  @Nullable
  public String getSource() {
    return source;
  }

  @Nullable
  public String[] getTags() {
    return tags;
  }

  @Override
  public String toString() {
    return "HistogramKey{" +
        "granularityOrdinal=" + granularityOrdinal +
        ", binId=" + binId +
        ", metric='" + metric + '\'' +
        ", source='" + source + '\'' +
        ", tags=" + Arrays.toString(tags) +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HistogramKey histogramKey = (HistogramKey) o;
    if (granularityOrdinal != histogramKey.granularityOrdinal) return false;
    if (binId != histogramKey.binId) return false;
    if (!metric.equals(histogramKey.metric)) return false;
    if (!Objects.equals(source, histogramKey.source)) return false;
    return Arrays.equals(tags, histogramKey.tags);
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + (int) granularityOrdinal;
    result = 31 * result + binId;
    result = 31 * result + metric.hashCode();
    result = 31 * result + (source != null ? source.hashCode() : 0);
    result = 31 * result + Arrays.hashCode(tags);
    return result;
  }

  /**
   * Unpacks tags into a map.
   */
  public Map<String, String> getTagsAsMap() {
    if (tags == null || tags.length == 0) {
      return ImmutableMap.of();
    }
    Map<String, String> annotations = new HashMap<>(tags.length / 2);
    for (int i = 0; i < tags.length - 1; i += 2) {
      annotations.put(tags[i], tags[i + 1]);
    }
    return annotations;
  }

  public long getBinTimeMillis() {
    return getBinDurationInMillis() * binId;
  }

  public long getBinDurationInMillis() {
    return Granularity.values()[granularityOrdinal].getInMillis();
  }

  void setGranularityOrdinal(byte granularityOrdinal) {
    this.granularityOrdinal = granularityOrdinal;
  }

  void setBinId(int binId) {
    this.binId = binId;
  }

  void setMetric(String metric) {
    this.metric = metric;
  }

  void setSource(@Nullable String source) {
    this.source = source;
  }

  void setTags(@Nullable String[] tags) {
    this.tags = tags;
  }
}
