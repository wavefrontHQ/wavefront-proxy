package com.wavefront.agent.logforwarder.ingestion.processors.model.event;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Event corresponds to one log message with all the fields
 * @param <String> KEY
 * @param <Object> VALUE
 */
public class Event extends LinkedHashMap<String, Object> {
  private static final long serialVersionUID = 1L;

  private boolean isDirty = false;

  @Override
  public Object put(String key, Object value) {
    this.isDirty = true;
    return super.put(key, value);
  }

  @Override
  public void putAll(Map<? extends String, ?> m) {
    this.isDirty = true;
    super.putAll(m);
  }

  @Override
  public Object putIfAbsent(String key, Object value) {
    this.isDirty = true;
    return super.putIfAbsent(key, value);
  }

  @Override
  public Object compute(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
    this.isDirty = true;
    return super.compute(key, remappingFunction);
  }

  @Override
  public Object computeIfAbsent(String key, Function<? super String, ?> mappingFunction) {
    this.isDirty = true;
    return super.computeIfAbsent(key, mappingFunction);
  }

  @Override
  public Object computeIfPresent(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
    this.isDirty = true;
    return super.computeIfPresent(key, remappingFunction);
  }

  @Override
  public boolean remove(Object key, Object value) {
    this.isDirty = true;
    return super.remove(key, value);
  }

  @Override
  public Object remove(Object key) {
    this.isDirty = true;
    return super.remove(key);
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
    this.isDirty = true;
    return super.removeEldestEntry(eldest);
  }

  public void setDirty(boolean dirty) {
    isDirty = dirty;
  }

  /**
   * @return true if any fields are added/updated/deleted from {@link com.vmware.ingestion.dataflow.Event}
   */
  public boolean isDirty() {
    return this.isDirty;
  }

  @Override
  public Event clone() {
    Event clone = new Event();
    clone.putAll(this);
    clone.isDirty = this.isDirty;
    return clone;
  }
}