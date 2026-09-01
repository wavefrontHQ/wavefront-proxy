package com.wavefront.agent;

import com.google.common.collect.Maps;
import com.wavefront.agent.api.APIContainer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.jetbrains.annotations.TestOnly;

public class TokenManager {
  private static final Logger logger = Logger.getLogger("proxy");

  /**
   * Separator used to create synthetic internal keys when the same tenant name is registered on
   * multiple servers (e.g. {@code "tenant1~2"}). Chosen to be extremely unlikely to appear in
   * real tenant names. The original alias (e.g. {@code "tenant1"}) is always preserved in
   * {@link #aliasToInternalKeys} for routing and validation.
   */
  public static final String SYNTHETIC_KEY_SEPARATOR = "~";

  // LinkedHashMap preserves insertion order so the central tenant (added first) is always
  // iterated first, making check-in behaviour deterministic across JVM restarts.
  private static final Map<String, TenantInfo> multicastingTenantList = Maps.newLinkedHashMap();
  private static List<TokenWorker.Scheduled> scheduledWorkers = new ArrayList<>();
  private static List<TokenWorker.External> externalWorkers = new ArrayList<>();

  /**
   * Tracks registered (server, tenantName) composite pairs to detect true duplicates.
   * Format: {@code "server|tenantName"}.
   */
  private static final Set<String> registeredServerTenantPairs = new HashSet<>();

  /**
   * Maps each original tenant alias (as specified in config) to every internal map key that
   * represents an endpoint for that alias across different servers.
   *
   * <p>Example: tenant {@code "Prod"} on two different servers:
   * <pre>
   *   "Prod" → ["Prod", "Prod~2"]
   * </pre>
   * Forward routing to {@code "Prod"} fans out to all keys in this list.
   */
  private static final Map<String, List<String>> aliasToInternalKeys = new LinkedHashMap<>();

  /**
   * Registers a tenant endpoint for multicasting.
   *
   * <p>Uniqueness is determined by the {@code (server, tenantName)} pair — not the token alone.
   * Tenant names are customer-facing labels; the same name can legitimately appear on different
   * cluster endpoints (e.g. both {@code wfdev} and {@code wfprod} may have a tenant named
   * {@code "Prod"}). The token is globally unique <em>within</em> a server, but not across
   * servers.
   *
   * <p>Collision rules:
   * <ul>
   *   <li><b>Same server + same name</b> — exact duplicate; the second entry is silently skipped
   *       (first registration wins, regardless of token).</li>
   *   <li><b>Same name + different server</b> — valid cross-cluster scenario; the second endpoint
   *       is registered under a synthetic internal key (e.g. {@code "tenant1~2"}) and data
   *       forwarded to {@code "tenant1"} will fan out to both endpoints.</li>
   * </ul>
   *
   * @param tenantName the customer-facing tenant alias (routing key in preprocessor forward rules)
   * @param tokenWorker authentication/credential worker for the tenant endpoint
   */
  public static void addTenant(String tenantName, TenantInfo tokenWorker) {
    String serverTenantPair = tokenWorker.getWFServer() + "|" + tenantName;
    if (registeredServerTenantPairs.contains(serverTenantPair)) {
      logger.fine("Tenant '" + tenantName + "' on server '" + tokenWorker.getWFServer()
          + "' is already registered — skipping duplicate entry.");
      return;
    }
    registeredServerTenantPairs.add(serverTenantPair);

    // Determine the internal map key for this registration.
    final String internalKey;
    if (!multicastingTenantList.containsKey(tenantName)) {
      // First endpoint for this alias — use the alias itself as the map key.
      internalKey = tenantName;
    } else {
      // Same alias on a different server — generate a synthetic key so both endpoints
      // coexist in the map and both receive forwarded data.
      int index = aliasToInternalKeys.getOrDefault(tenantName, Collections.emptyList()).size() + 1;
      internalKey = tenantName + SYNTHETIC_KEY_SEPARATOR + index;
      logger.info("Tenant '" + tenantName + "' is already registered on a different server. "
          + "Registering additional endpoint as '" + internalKey + "'. "
          + "Forward routing to '" + tenantName + "' will fan out to all registered endpoints.");
    }

    multicastingTenantList.put(internalKey, tokenWorker);
    aliasToInternalKeys.computeIfAbsent(tenantName, k -> new ArrayList<>()).add(internalKey);

    if (tokenWorker instanceof TokenWorker.Scheduled) {
      scheduledWorkers.add((TokenWorker.Scheduled) tokenWorker);
    }
    if (tokenWorker instanceof TokenWorker.External) {
      externalWorkers.add((TokenWorker.External) tokenWorker);
    }
  }

  /**
   * Returns all internal map keys registered under the given tenant alias.
   * Used for fan-out routing: when a metric targets {@code "tenant1"}, all registered endpoints
   * for that alias (across different servers) should receive the data.
   *
   * @param tenantAlias the original alias as used in forward routing rules
   * @return immutable list of internal keys; empty if the alias is not registered
   */
  public static List<String> getInternalKeysForName(String tenantAlias) {
    return Collections.unmodifiableList(
        aliasToInternalKeys.getOrDefault(tenantAlias, Collections.emptyList()));
  }

  /**
   * Returns the set of all registered original tenant aliases (as specified in config).
   * Does <em>not</em> include synthetic internal keys (e.g. {@code "tenant1~2"}).
   * Intended for forward-rule target validation.
   */
  public static Set<String> getRegisteredAliases() {
    return Collections.unmodifiableSet(aliasToInternalKeys.keySet());
  }

  public static void start(APIContainer apiContainer) {
    externalWorkers.forEach(external -> external.setAPIContainer(apiContainer));
    scheduledWorkers.forEach(tenantInfo -> tenantInfo.run());
  }

  public static Map<String, TenantInfo> getMulticastingTenantList() {
    return multicastingTenantList;
  }

  @TestOnly
  public static void reset() {
    externalWorkers.clear();
    scheduledWorkers.clear();
    multicastingTenantList.clear();
    registeredServerTenantPairs.clear();
    aliasToInternalKeys.clear();
  }
}
