package com.wavefront.agent.handlers;

import com.wavefront.data.ReportableEntityType;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * An immutable unique identifier for a handler pipeline (type of objects handled + port/handle name)
 *
 * @author vasily@wavefront.com
 */
public class HandlerKey {
  private final ReportableEntityType entityType;
  @Nonnull
  private final String handle;

  private HandlerKey(ReportableEntityType entityType, @Nonnull String handle) {
    this.entityType = entityType;
    this.handle = handle;
  }

  public ReportableEntityType getEntityType() {
    return entityType;
  }

  @Nonnull
  public String getHandle() {
    return handle;
  }

  public static HandlerKey of(ReportableEntityType entityType, @Nonnull String handle) {
    return new HandlerKey(entityType, handle);
  }

  @Override
  public int hashCode() {
    return 31 * entityType.hashCode() + handle.hashCode();
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
    return this.entityType + "." + this.handle;
  }
}
