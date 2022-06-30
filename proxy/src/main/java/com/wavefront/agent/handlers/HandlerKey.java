package com.wavefront.agent.handlers;

import com.wavefront.data.ReportableEntityType;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An immutable unique identifier for a handler pipeline (type of objects handled + port/handle name
 * + tenant name)
 *
 * @author vasily@wavefront.com
 */
public class HandlerKey {
  private final ReportableEntityType entityType;
  @Nonnull private final String handle;
  @Nullable private final String tenantName;

  private HandlerKey(
      ReportableEntityType entityType, @Nonnull String handle, @Nullable String tenantName) {
    this.entityType = entityType;
    this.handle = handle;
    this.tenantName = tenantName;
  }

  public static String generateTenantSpecificHandle(String handle, @Nonnull String tenantName) {
    return handle + "." + tenantName;
  }

  public ReportableEntityType getEntityType() {
    return entityType;
  }

  @Nonnull
  public String getHandle() {
    return handle + (this.tenantName == null ? "" : "." + this.tenantName);
  }

  public String getTenantName() {
    return this.tenantName;
  }

  public static HandlerKey of(ReportableEntityType entityType, @Nonnull String handle) {
    return new HandlerKey(entityType, handle, null);
  }

  public static HandlerKey of(
      ReportableEntityType entityType, @Nonnull String handle, @Nonnull String tenantName) {
    return new HandlerKey(entityType, handle, tenantName);
  }

  @Override
  public int hashCode() {
    return 31 * 31 * entityType.hashCode()
        + 31 * handle.hashCode()
        + (this.tenantName == null ? 0 : this.tenantName.hashCode());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HandlerKey that = (HandlerKey) o;
    if (!entityType.equals(that.entityType)) return false;
    if (!Objects.equals(handle, that.handle)) return false;
    if (!Objects.equals(tenantName, that.tenantName)) return false;
    return true;
  }

  @Override
  public String toString() {
    return this.entityType
        + "."
        + this.handle
        + (this.tenantName == null ? "" : "." + this.tenantName);
  }
}
