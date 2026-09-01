package com.wavefront.agent.preprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.ProxyCheckInScheduler;
import com.wavefront.agent.TokenManager;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.api.agent.preprocessor.PreprocessorConfigManager;
import com.wavefront.api.agent.preprocessor.ReportableEntityPreprocessor;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Extends the {@link PreprocessorConfigManager} from java-lib to add Wavefront-proxy-specific
 * behaviour for the <em>central (default) tenant's</em> rule set:
 *
 * <ul>
 *   <li>Loading rules from a local YAML file or from the Wavefront front-end.</li>
 *   <li>Hot-reload monitoring (5-second polling via {@link #setUpConfigFileMonitoring}).</li>
 *   <li>Validation that forward-rule target tenant names are registered in
 *       {@link TokenManager} ({@link ForwardRuleValidator}).</li>
 * </ul>
 *
 * <p>In multi-tenant mode only the default tenant's preprocessor rules are applied; rules from
 * non-default tenants are ignored entirely at the call-site ({@link com.wavefront.agent.PushAgent}).
 *
 * <p>Created by Vasily on 9/15/16.
 */
public class ProxyPreprocessorConfigManager extends PreprocessorConfigManager {

  private static final Logger logger =
      Logger.getLogger(ProxyPreprocessorConfigManager.class.getCanonicalName());
  private static final Counter configReloads =
      Metrics.newCounter(new MetricName("preprocessor", "", "config-reloads.successful"));
  private static final Counter failedConfigReloads =
      Metrics.newCounter(new MetricName("preprocessor", "", "config-reloads.failed"));

  private final Supplier<Long> timeSupplier;

  @VisibleForTesting
  public Map<String, ReportableEntityPreprocessor> userPreprocessors;

  private volatile long userPreprocessorsTs;
  private static String proxyConfigRules;

  /**
   * Human-readable alias for the central tenant (e.g. {@code "Localdev"}). Included in the valid
   * tenant-name set when validating forward-rule targets so that rules may reference the central
   * tenant by either its internal constant or this alias.
   */
  @Nullable
  private volatile String defaultTenantAlias;

  public ProxyPreprocessorConfigManager() {
    this(System::currentTimeMillis);
  }

  /** @param timeSupplier Supplier for current time (in millis). */
  @VisibleForTesting
  ProxyPreprocessorConfigManager(@Nonnull Supplier<Long> timeSupplier) {
    this.timeSupplier = timeSupplier;
    userPreprocessorsTs = timeSupplier.get();
    userPreprocessors = Collections.emptyMap();
  }

  /**
   * Sets the human-readable alias for the central tenant (e.g. {@code "Localdev"}). Used when
   * validating forward-rule target tenant names so that rules may reference the central tenant by
   * alias as well as by its internal {@link APIContainer#CENTRAL_TENANT_NAME} constant.
   *
   * @param defaultTenantAlias the alias, or {@code null} if none is configured
   */
  public void setDefaultTenant(@Nullable String defaultTenantAlias) {
    this.defaultTenantAlias = defaultTenantAlias;
  }

  /**
   * Schedules periodic checks for config file modification timestamp and performs hot-reload.
   *
   * @param fileName              path to the preprocessor rule file.
   * @param fileCheckIntervalMillis timestamp check interval in milliseconds.
   */
  public void setUpConfigFileMonitoring(String fileName, int fileCheckIntervalMillis) {
    new Timer("Timer-preprocessor-configmanager")
        .schedule(
            new TimerTask() {
              @Override
              public void run() {
                loadFileIfModified(fileName);
              }
            },
            fileCheckIntervalMillis,
            fileCheckIntervalMillis);
  }

  @VisibleForTesting
  void loadFileIfModified(String fileName) {
    if (ProxyCheckInScheduler.isRulesSetInFE.get()) return;
    try {
      File file = new File(fileName);
      long lastModified = file.lastModified();
      if (lastModified > userPreprocessorsTs) {
        logger.info("File " + file + " has been modified on disk, reloading preprocessor rules");
        loadFile(fileName);
        configReloads.inc();
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Unable to load preprocessor rules", e);
      failedConfigReloads.inc();
    }
  }

  /**
   * Loads central-tenant preprocessor rules from a local YAML file. After parsing, validates that
   * all forward-rule target tenant names are registered in {@link TokenManager}.
   *
   * @param filename path to the YAML rule file
   * @throws FileNotFoundException when the file does not exist
   */
  public void loadFile(String filename) throws FileNotFoundException {
    File file = new File(filename);
    super.loadFromStream(new FileInputStream(file));
    validateCentralTenantForwardRuleTargets();
    proxyConfigRules = getFileRules(filename);
    ProxyCheckInScheduler.preprocessorRulesNeedUpdate.set(true);
  }

  /**
   * Loads central-tenant preprocessor rules from a YAML string received from the Wavefront
   * front-end. After parsing, validates forward-rule target tenant names.
   *
   * @param yamlRules YAML rule string from the FE
   */
  public void loadFERules(String yamlRules) {
    logger.info("New preprocessor rules detected! Loading preprocessor rules from FE Configuration");
    InputStream rulesInputStream = new ByteArrayInputStream(yamlRules.getBytes(StandardCharsets.UTF_8));
    super.loadFromStream(rulesInputStream);
    validateCentralTenantForwardRuleTargets();
    proxyConfigRules = yamlRules;
    ProxyCheckInScheduler.preprocessorRulesNeedUpdate.set(true);
  }

  /**
   * Validates that all tenant names referenced in forward/spanForward/logForward rules of the
   * central tenant's rule set are registered in {@link TokenManager}. Includes the central-tenant
   * constant, all multicasting tenant names from {@link TokenManager}, and the configured
   * {@link #defaultTenantAlias} (if any).
   */
  private void validateCentralTenantForwardRuleTargets() {
    // Use original aliases (not synthetic internal keys like "tenant1~2") so that forward
    // rules referencing "tenant1" are accepted even when that name is registered on multiple
    // servers.  getRegisteredAliases() returns the customer-facing names as configured.
    Set<String> registeredAliases = TokenManager.getRegisteredAliases();

    // Full acceptance set: internal "central" key + defaultTenant alias + all multicastingTenants.
    // Used for the boolean validity check — operators may write either "central" or the alias.
    Set<String> validTenantNames = new HashSet<>(registeredAliases);
    validTenantNames.add(APIContainer.CENTRAL_TENANT_NAME);
    String alias = this.defaultTenantAlias;
    if (alias != null) {
      validTenantNames.add(alias);
    }

    // Display set shown in error messages: only the actual multicastingTenant names from
    // TokenManager, excluding "central" and the defaultTenant alias.
    // "central" and its alias are both names for the *same* primary cluster; showing them
    // both as if they were two separate tenants confuses operators.
    Set<String> multicastingTenantNames = new HashSet<>(registeredAliases);
    multicastingTenantNames.remove(APIContainer.CENTRAL_TENANT_NAME);

    // "Multi-tenant mode" means at least one multicastingTenant is registered beyond central.
    boolean isMultiTenantMode = registeredAliases.size() > 1;

    // In multi-tenant mode, defaultTenant must always be set when a preprocessor file is loaded.
    // The purpose of multicastingTenants is data routing, which requires knowing the primary
    // cluster's friendly alias; without it, the configuration intent is ambiguous and operators
    // cannot route data back to the primary cluster by a human-readable name.
    if (alias == null && isMultiTenantMode) {
      throw new IllegalArgumentException(
          "'defaultTenant' is missing from config.ini. In multi-tenant mode"
              + " 'defaultTenant=<tenantName>' must be set to identify the primary cluster's"
              + " friendly alias before preprocessor rules can be loaded."
              + " Registered multicastingTenants: "
              + multicastingTenantNames
              + ". Example: add 'defaultTenant=Localdev' to config.ini.");
    }

    try {
      ForwardRuleValidator.validateForwardRuleTargetTenants(
          getUserPreprocessors(), validTenantNames, multicastingTenantNames);
    } catch (IllegalArgumentException ex) {
      if (alias == null) {
        // defaultTenant not configured — the alias is absent from the valid set.
        throw new IllegalArgumentException(
            ex.getMessage()
                + ". If this tenant name is your primary cluster's alias, add"
                + " 'defaultTenant=<tenantName>' to config.ini so the proxy recognises it"
                + " as a valid forward-rule target.",
            ex);
      }
      if (!isMultiTenantMode) {
        // defaultTenant is set but no multicastingTenants are configured.
        // The forward rule references a name that can only belong to a multicastingTenant.
        throw new IllegalArgumentException(
            ex.getMessage()
                + ". Multi-tenant routing is not configured in config.ini."
                + " Forward rules can only target registered multicastingTenants."
                + " Add 'multicastingTenants=N' and the corresponding"
                + " 'multicastingTenantName_X=<name>', 'multicastingServer_X=<url>',"
                + " 'multicastingToken_X=<token>' entries to config.ini"
                + " for each tenant referenced in the preprocessor forward rules.",
            ex);
      }
      throw ex;
    }
  }

  public static String getProxyConfigRules() {
    return proxyConfigRules;
  }

  public static String getFileRules(String filename) {
    try {
      if (filename == null || filename.isEmpty()) return null;
      return new String(Files.readAllBytes(Paths.get(filename)), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Unable to read file rules as string", e);
    }
  }

  @VisibleForTesting
  public static void clearProxyConfigRules() {
    proxyConfigRules = null;
  }
}
