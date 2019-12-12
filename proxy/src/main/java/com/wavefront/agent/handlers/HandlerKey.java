package com.wavefront.agent.handlers;

import com.wavefront.data.ReportableEntityType;

import javax.validation.constraints.NotNull;
import java.util.Objects;

/**
 * An immutable unique identifier for a handler pipeline (type of objects handled + port/handle name)
 *
 * @author vasily@wavefront.com
 */
public class HandlerKey {
  private final ReportableEntityType entityType;
  @NotNull
  private final String handle;

  private HandlerKey(ReportableEntityType entityType, @NotNull String handle) {
    this.entityType = entityType;
    this.handle = handle;
  }

  public ReportableEntityType getEntityType() {
    return entityType;
  }

  public String getHandle() {
    return handle;
  }

  public static HandlerKey of(ReportableEntityType entityType, @NotNull String handle) {
    return new HandlerKey(entityType, handle);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HandlerKey that = (HandlerKey) o;
    if (!entityType.equals(that.entityType)) return false;
    if (!Objects.equals(handle, that.handle)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "HandlerKey{entityType=" + this.entityType + ", handle=" + this.handle + "}";
  }

}
