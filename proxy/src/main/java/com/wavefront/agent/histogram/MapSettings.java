package com.wavefront.agent.histogram;

/**
 * Stores settings ChronicleMap has been initialized with to trigger map re-creation when settings change
 * (since ChronicleMap doesn't persist init values for entries/avgKeySize/avgValueSize)
 *
 * @author vasily@wavefront.com
 */
public class MapSettings {
  private Class<?> keyClass;
  private Class<?> valueClass;
  private Class<?> keyMarshaller;
  private Class<?> valueMarshaller;
  private long entries;
  private double avgKeySize;
  private double avgValueSize;

  @SuppressWarnings("unused")
  MapSettings() {
  }

  public MapSettings(Class<?> keyClass, Class<?> valueClass, Class<?> keyMarshaller,
                     Class<?> valueMarshaller, long entries, double avgKeySize,
                     double avgValueSize) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.keyMarshaller = keyMarshaller;
    this.valueMarshaller = valueMarshaller;
    this.entries = entries;
    this.avgKeySize = avgKeySize;
    this.avgValueSize = avgValueSize;
  }

  public Class<?> getKeyClass() {
    return keyClass;
  }

  public Class<?> getValueClass() {
    return valueClass;
  }

  public Class<?> getKeyMarshaller() {
    return keyMarshaller;
  }

  public Class<?> getValueMarshaller() {
    return valueMarshaller;
  }

  public long getEntries() {
    return entries;
  }

  public double getAvgKeySize() {
    return avgKeySize;
  }

  public double getAvgValueSize() {
    return avgValueSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MapSettings that = (MapSettings)o;

    return (this.keyClass == that.keyClass
        && this.valueClass == that.valueClass
        && this.keyMarshaller == that.keyMarshaller
        && this.valueMarshaller == that.valueMarshaller
        && this.entries == that.entries
        && this.avgKeySize == that.avgKeySize
        && this.avgValueSize == that.avgValueSize);
  }

  @Override
  public String toString() {
    return "MapSettings{" +
        "keyClass=" + (keyClass == null ? "(null)" : keyClass.getName()) +
        ", valueClass=" + (valueClass == null ? "(null)" : valueClass.getName()) +
        ", keyMarshaller=" + (keyMarshaller == null ? "(null)" : keyMarshaller.getName()) +
        ", valueMarshaller=" + (valueMarshaller == null ? "(null)" : valueMarshaller.getName()) +
        ", entries=" + entries +
        ", avgKeySize=" + avgKeySize +
        ", avgValueSize=" + avgValueSize +
        '}';
  }
}
