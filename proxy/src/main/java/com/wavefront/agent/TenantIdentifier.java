package com.wavefront.agent;

import java.util.Objects;

/**
 * Immutable value object that wraps a Wavefront tenant name, replacing the bare {@link String}
 * constants that were previously used as map keys and method parameters throughout the check-in
 * layer. Using a dedicated type instead of a plain string prevents accidental substitution of
 * unrelated strings (port numbers, config file paths, etc.) where a tenant name is expected, and
 * allows future additions of typed behaviour (e.g. {@link #isCentral()}) without touching callers.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * // preferred — typed constant for the primary tenant
 * Map<TenantIdentifier, UUID> proxyIdByTenant = new LinkedHashMap<>();
 * proxyIdByTenant.put(TenantIdentifier.CENTRAL, centralProxyId);
 *
 * // converting a string tenant name from configuration
 * TenantIdentifier masterTenant = TenantIdentifier.of("Master");
 * proxyIdByTenant.put(masterTenant, masterProxyId);
 * }</pre>
 *
 * <p>Instances are safe to use as {@link java.util.Map} keys: {@link #equals} and
 * {@link #hashCode} are based solely on the tenant name string.
 */
public final class TenantIdentifier {

  /**
   * Typed constant for the proxy's central (default/primary) tenant. Corresponds to
   * {@link com.wavefront.agent.api.APIContainer#CENTRAL_TENANT_NAME} internally; callers should
   * always prefer this constant over constructing one via {@link #of(String)} with the raw string.
   */
  public static final TenantIdentifier CENTRAL =
      new TenantIdentifier(com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME);

  private final String tenantName;

  private TenantIdentifier(String tenantName) {
    this.tenantName = Objects.requireNonNull(tenantName, "tenantName must not be null");
  }

  /**
   * Returns a {@link TenantIdentifier} for the given name. When {@code tenantName} matches
   * {@link com.wavefront.agent.api.APIContainer#CENTRAL_TENANT_NAME} the {@link #CENTRAL} sentinel
   * is returned so that identity comparison ({@code ==}) works for the common case.
   *
   * @param tenantName the tenant name as it appears in proxy configuration or
   *     {@link TokenManager}; must not be {@code null}
   * @return a {@code TenantIdentifier} wrapping the given name
   */
  public static TenantIdentifier of(String tenantName) {
    Objects.requireNonNull(tenantName, "tenantName must not be null");
    if (tenantName.equals(com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME)) {
      return CENTRAL;
    }
    return new TenantIdentifier(tenantName);
  }

  /**
   * Returns the raw tenant name string, for interoperability with APIs that still use
   * {@link String}-keyed maps (e.g. {@link com.wavefront.agent.handlers.SenderTask} maps).
   */
  public String getTenantName() {
    return tenantName;
  }

  /**
   * Returns {@code true} when this identifier represents the central (default) tenant — i.e. when
   * it equals {@link #CENTRAL}.
   */
  public boolean isCentral() {
    return CENTRAL.tenantName.equals(this.tenantName);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (!(other instanceof TenantIdentifier)) return false;
    return tenantName.equals(((TenantIdentifier) other).tenantName);
  }

  @Override
  public int hashCode() {
    return tenantName.hashCode();
  }

  /** Returns the raw tenant name — same as {@link #getTenantName()}. */
  @Override
  public String toString() {
    return tenantName;
  }
}
