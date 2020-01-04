package com.wavefront.agent.histogram;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Stores settings ChronicleMap has been initialized with to trigger map re-creation when settings change
 * (since ChronicleMap doesn't persist init values for entries/avgKeySize/avgValueSize)
 *
 * @author vasily@wavefront.com
 */
public class MapSettings {
  private long entries;
  private double avgKeySize;
  private double avgValueSize;

  @SuppressWarnings("unused")
  private MapSettings() {
  }

  public MapSettings(long entries, double avgKeySize, double avgValueSize) {
    this.entries = entries;
    this.avgKeySize = avgKeySize;
    this.avgValueSize = avgValueSize;
  }

  @JsonProperty
  public long getEntries() {
    return entries;
  }

  @JsonProperty
  public double getAvgKeySize() {
    return avgKeySize;
  }

  @JsonProperty
  public double getAvgValueSize() {
    return avgValueSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MapSettings that = (MapSettings)o;

    return (this.entries == that.entries
        && this.avgKeySize == that.avgKeySize
        && this.avgValueSize == that.avgValueSize);
  }
}
