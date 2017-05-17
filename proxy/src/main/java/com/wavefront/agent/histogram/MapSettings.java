package com.wavefront.agent.histogram;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Stores settings ChronicleMap has been initialized with to trigger map re-creation when settings change
 * (since ChronicleMap doesn't persist init values for entries/avgKeySize/avgValueSize)
 *
 * @author vasily@wavefront.com
 */
public class MapSettings {
  private final Class keyClass;
  private final Class valueClass;
  private final Class keyMarshaller;
  private final Class valueMarshaller;
  private final long entries;
  private final double avgKeySize;
  private final double avgValueSize;

  public MapSettings(Class keyClass, Class valueClass, Class keyMarshaller, Class valueMarshaller,
                     long entries, double avgKeySize, double avgValueSize) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.keyMarshaller = keyMarshaller;
    this.valueMarshaller = valueMarshaller;
    this.entries = entries;
    this.avgKeySize = avgKeySize;
    this.avgValueSize = avgValueSize;
  }

  public Class getKeyClass() {
    return keyClass;
  }

  public Class getValueClass() {
    return valueClass;
  }

  public Class getKeyMarshaller() {
    return keyMarshaller;
  }

  public Class getValueMarshaller() {
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

  public static class ClassNameSerializer implements JsonSerializer<Class>, JsonDeserializer<Class> {
    @Override
    public JsonElement serialize(Class src, Type type, JsonSerializationContext context) {
      return context.serialize(src.getName());
    }
    @Override
    public Class deserialize(JsonElement src, Type type, JsonDeserializationContext context) {
      try {
        return Class.forName(src.getAsString());
      } catch (ClassNotFoundException e) {
        return null;
      }
    }

  }


}
