package com.wavefront.agent;

import com.wavefront.agent.api.APIContainer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProxySendConfigScheduler {
  private static final Logger logger =
      Logger.getLogger(ProxySendConfigScheduler.class.getCanonicalName());
  private boolean successful = false;
  private final ScheduledExecutorService executor;
  private final Runnable task;

  /**
   * Backwards-compatible single-tenant constructor. Sends config only for the central tenant.
   */
  public ProxySendConfigScheduler(
      APIContainer apiContainer, UUID proxyId, ProxyConfig proxyConfig) {
    this(apiContainer, Map.of(TenantIdentifier.CENTRAL, proxyId), proxyConfig);
  }

  /**
   * Multi-tenant constructor. Sends {@code proxySaveConfig} to every tenant's proxy object so
   * that the wf-system Proxy Configurator shows the full listener/buffer configuration (Metrics,
   * Histograms, Traces, Source Tags, Logs, etc.) for every tenant, not just the central one.
   *
   * <p>All tenants share the same physical listener ports and proxy settings; broadcasting the
   * config makes the UI consistent regardless of which tenant context the operator is viewing.
   */
  public ProxySendConfigScheduler(
      APIContainer apiContainer,
      Map<TenantIdentifier, UUID> tenantProxyIds,
      ProxyConfig proxyConfig) {
    executor = Executors.newScheduledThreadPool(1);
    task =
        () -> {
          boolean allSucceeded = true;
          for (Map.Entry<TenantIdentifier, UUID> entry : tenantProxyIds.entrySet()) {
            String tenantName = entry.getKey().getTenantName();
            UUID tenantProxyId = entry.getValue();
            try {
              apiContainer.setCurrentTenantForSave(tenantName);
              apiContainer
                  .getProxyV2APIForTenant(tenantName)
                  .proxySaveConfig(tenantProxyId, proxyConfig.getJsonConfig());
              logger.info("Saved proxy config for tenant '" + tenantName
                  + "' (proxy UUID " + tenantProxyId + ").");
            } catch (javax.ws.rs.NotFoundException ex) {
              // Older server versions may not support this endpoint; treat as success.
              logger.info("'proxySaveConfig' endpoint not found for tenant '" + tenantName
                  + "' — server may be an older version; skipping config upload for this tenant.");
            } catch (javax.ws.rs.ClientErrorException ex) {
              // 401/403: token may lack proxy-management permission on this tenant's server.
              // Log but do not retry — the token won't change at runtime.
              logger.warning("'proxySaveConfig' rejected for tenant '" + tenantName
                  + "' (proxy UUID " + tenantProxyId + ")"
                  + ": HTTP " + ex.getResponse().getStatus()
                  + " — token may lack proxy-management permission."
                  + " Config will not appear in the wf-system UI for this tenant.");
            } catch (Throwable e) {
              logger.severe("Can't send Proxy configuration for tenant '" + tenantName
                  + "', retrying in 60 seconds. " + e.getMessage());
              logger.log(Level.FINE, "Exception: ", e);
              allSucceeded = false;
            } finally {
              apiContainer.clearCurrentTenantForSave();
            }
          }
          if (allSucceeded) {
            successful = true;
            logger.info("Configuration sent to the server successfully.");
          }

          if (successful) {
            executor.shutdown();
          }
        };
  }

  public void start() {
    executor.scheduleAtFixedRate(task, 0, 60, TimeUnit.SECONDS);
  }
}
