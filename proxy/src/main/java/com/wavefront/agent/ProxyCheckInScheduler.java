package com.wavefront.agent;

import static com.wavefront.common.Utils.getBuildVersion;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.preprocessor.ProxyPreprocessorConfigManager;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.metrics.JsonMetricsGenerator;
import com.yammer.metrics.Metrics;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ProcessingException;
import org.apache.commons.lang.StringUtils;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Registers the proxy with the back-end, sets up regular "check-ins" (every minute), transmits
 * proxy metrics to the back-end.
 *
 * @author vasily@wavefront.com
 */
public class ProxyCheckInScheduler {
  private static final Logger logger = Logger.getLogger("proxy");
  private static final int MAX_CHECKIN_ATTEMPTS = 5;

  /**
   * A unique value (a random hexadecimal string), assigned at proxy start-up, to be reported with
   * all ~proxy metrics as a "processId" point tag to prevent potential ~proxy metrics collisions
   * caused by users spinning up multiple proxies with duplicate names.
   */
  private static final String ID = Integer.toHexString((int) (Math.random() * Integer.MAX_VALUE));

  private final UUID proxyId;
  private final ProxyConfig proxyConfig;
  private final APIContainer apiContainer;
  private final BiConsumer<String, AgentConfiguration> agentConfigurationConsumer;
  private final Runnable shutdownHook;
  private final Runnable truncateBacklog;
  private final AtomicInteger retries = new AtomicInteger(0);
  private final AtomicLong successfulCheckIns = new AtomicLong(0);
  /** Executors for support tasks. */
  private final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(2, new NamedThreadFactory("proxy-configuration"));

  private String serverEndpointUrl = null;
  private volatile JsonNode agentMetrics;
  private boolean retryImmediately = false;

  // check if preprocessor rules need to be sent to update BE
  public static AtomicBoolean preprocessorRulesNeedUpdate = new AtomicBoolean(false);
  // check if rules are set from FE/API
  public static AtomicBoolean isRulesSetInFE = new AtomicBoolean(false);

  /**
   * @param proxyId Proxy UUID.
   * @param proxyConfig Proxy settings.
   * @param apiContainer API container object.
   * @param agentConfigurationConsumer Configuration processor, invoked after each successful
   *     configuration fetch.
   * @param shutdownHook Invoked when proxy receives a shutdown directive from the back-end.
   */
  public ProxyCheckInScheduler(
      UUID proxyId,
      ProxyConfig proxyConfig,
      APIContainer apiContainer,
      BiConsumer<String, AgentConfiguration> agentConfigurationConsumer,
      Runnable shutdownHook,
      Runnable truncateBacklog) {
    this.proxyId = proxyId;
    this.proxyConfig = proxyConfig;
    this.apiContainer = apiContainer;
    this.agentConfigurationConsumer = agentConfigurationConsumer;
    this.shutdownHook = shutdownHook;
    this.truncateBacklog = truncateBacklog;

    updateProxyMetrics();

    Map<String, AgentConfiguration> configList = checkin();
    new ProxySendConfigScheduler(apiContainer, proxyId, proxyConfig).start();

    if (configList == null && retryImmediately) {
      // immediately retry check-ins if we need to re-attempt
      // due to changing the server endpoint URL
      updateProxyMetrics();
      configList = checkin();
      sendPreprocessorRules();
    }
    if (configList != null && !configList.isEmpty()) {
      logger.info("initial configuration is available, setting up proxy");
      for (Map.Entry<String, AgentConfiguration> configEntry : configList.entrySet()) {
        agentConfigurationConsumer.accept(configEntry.getKey(), configEntry.getValue());
        successfulCheckIns.incrementAndGet();
      }
    }
  }

  /** Set up and schedule regular check-ins. */
  public void scheduleCheckins() {
    logger.info("scheduling regular check-ins");
    executor.scheduleAtFixedRate(this::updateProxyMetrics, 60, 60, TimeUnit.SECONDS);
    executor.scheduleWithFixedDelay(this::updateConfiguration, 0, 1, TimeUnit.SECONDS);
  }

  /**
   * Returns the number of successful check-ins.
   *
   * @return true if this proxy had at least one successful check-in.
   */
  public long getSuccessfulCheckinCount() {
    return successfulCheckIns.get();
  }

  /** Stops regular check-ins. */
  public void shutdown() {
    executor.shutdown();
  }

  /**
   * Initial sending of preprocessor rules. Will always check local location for preprocessor rule.
   */
  public void sendPreprocessorRules() {
    if (preprocessorRulesNeedUpdate.getAndSet(false)) {
      try {
        JsonNode rulesNode = createRulesNode(ProxyPreprocessorConfigManager.getProxyConfigRules(), null);
        apiContainer
                .getProxyV2APIForTenant(APIContainer.CENTRAL_TENANT_NAME)
                .proxySavePreprocessorRules(
                        proxyId,
                        rulesNode
                );
      } catch (javax.ws.rs.NotFoundException ex) {
        logger.warning("'proxySavePreprocessorRules' api end point not found");
      }
    }
  }

  /** Send preprocessor rules */
  private void sendPreprocessorRules(AgentConfiguration agentConfiguration) {
    if (preprocessorRulesNeedUpdate.getAndSet(false)) {
      String preprocessorRules = null;
      if (agentConfiguration.getPreprocessorRules() != null) {
        // reading rules from BE if sent from BE
        preprocessorRules = agentConfiguration.getPreprocessorRules();
      } else {
        // reading local file's rule
        preprocessorRules = ProxyPreprocessorConfigManager.getProxyConfigRules();
      }
      try {
        JsonNode rulesNode = createRulesNode(preprocessorRules, agentConfiguration.getPreprocessorRulesId());
        apiContainer
                .getProxyV2APIForTenant(APIContainer.CENTRAL_TENANT_NAME)
                .proxySavePreprocessorRules(
                        proxyId,
                        rulesNode
                );
      } catch (javax.ws.rs.NotFoundException ex) {
        logger.warning("'proxySavePreprocessorRules' api end point not found");
      }
    }
  }

  private JsonNode createRulesNode(String preprocessorRules, String proxyId) {
    Map<String, String> fieldsMap = new HashMap<>();
    fieldsMap.put("proxyRules", preprocessorRules);
    if (proxyConfig.getPreprocessorConfigFile() != null) fieldsMap.put("proxyRulesFilePath", proxyConfig.getPreprocessorConfigFile());
    if (proxyId != null) fieldsMap.put("proxyRulesId", proxyId);

    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.valueToTree(fieldsMap);
  }

  /**
   * Perform agent check-in and fetch configuration of the daemon from remote server.
   *
   * @return Fetched configuration map {tenant_name: config instance}. {@code null} if the
   *     configuration is invalid.
   */
  private Map<String, AgentConfiguration> checkin() {
    Map<String, AgentConfiguration> configurationList = Maps.newHashMap();
    JsonNode agentMetricsWorkingCopy;
    synchronized (executor) {
      if (agentMetrics == null) return null;
      agentMetricsWorkingCopy = agentMetrics;
      agentMetrics = null;
      if (retries.incrementAndGet() > MAX_CHECKIN_ATTEMPTS) return null;
    }
    // MONIT-25479: check-in for central and multicasting tenants / clusters
    Map<String, TenantInfo> multicastingTenantList = TokenManager.getMulticastingTenantList();
    // Initialize tenantName and multicastingTenantProxyConfig here to track current checking
    // tenant for better exception handling message
    String tenantName = APIContainer.CENTRAL_TENANT_NAME;
    TenantInfo multicastingTenantProxyConfig =
        multicastingTenantList.get(APIContainer.CENTRAL_TENANT_NAME);
    try {
      AgentConfiguration multicastingConfig;
      for (Map.Entry<String, TenantInfo> multicastingTenantEntry :
          multicastingTenantList.entrySet()) {
        tenantName = multicastingTenantEntry.getKey();
        multicastingTenantProxyConfig = multicastingTenantEntry.getValue();
        logger.info("Checking in tenants: " + multicastingTenantProxyConfig.getWFServer());
        multicastingConfig =
            apiContainer
                .getProxyV2APIForTenant(tenantName)
                .proxyCheckin(
                    proxyId,
                    "Bearer " + multicastingTenantProxyConfig.getBearerToken(),
                    proxyConfig.getHostname()
                        + (multicastingTenantList.size() > 1 ? "-multi_tenant" : ""),
                    proxyConfig.getProxyname(),
                    getBuildVersion(),
                    System.currentTimeMillis(),
                    agentMetricsWorkingCopy,
                    proxyConfig.isEphemeral());
        configurationList.put(tenantName, multicastingConfig);
      }
      agentMetricsWorkingCopy = null;
    } catch (ClientErrorException ex) {
      agentMetricsWorkingCopy = null;
      switch (ex.getResponse().getStatus()) {
        case 401:
          checkinError(
              "HTTP 401 Unauthorized: Please verify that your server and token settings"
                  + " are correct and that the token has Proxy Management permission!");
          if (successfulCheckIns.get() == 0) {
            throw new RuntimeException("Aborting start-up");
          }
          break;
        case 403:
          checkinError(
              "HTTP 403 Forbidden: Please verify that your token has Proxy Management "
                  + "permission!");
          if (successfulCheckIns.get() == 0) {
            throw new RuntimeException("Aborting start-up");
          }
          break;
        case 404:
        case 405:
          String serverUrl = multicastingTenantProxyConfig.getWFServer().replaceAll("/$", "");
          if (successfulCheckIns.get() == 0 && !retryImmediately && !serverUrl.endsWith("/api")) {
            this.serverEndpointUrl = serverUrl + "/api/";
            checkinError(
                "Possible server endpoint misconfiguration detected, attempting to use "
                    + serverEndpointUrl);
            apiContainer.updateServerEndpointURL(tenantName, serverEndpointUrl);
            retryImmediately = true;
            return null;
          }
          String secondaryMessage =
              serverUrl.endsWith("/api")
                  ? "Current setting: " + multicastingTenantProxyConfig.getWFServer()
                  : "Server endpoint URLs normally end with '/api/'. Current setting: "
                      + multicastingTenantProxyConfig.getBearerToken();
          checkinError(
              "HTTP "
                  + ex.getResponse().getStatus()
                  + ": Misconfiguration detected, "
                  + "please verify that your server setting is correct. "
                  + secondaryMessage);
          if (successfulCheckIns.get() == 0) {
            throw new RuntimeException("Aborting start-up");
          }
          break;
        case 407:
          checkinError(
              "HTTP 407 Proxy Authentication Required: Please verify that "
                  + "proxyUser and proxyPassword settings are correct and make sure your HTTP proxy"
                  + " is not rate limiting!");
          if (successfulCheckIns.get() == 0) {
            throw new RuntimeException("Aborting start-up");
          }
          break;
        case 429:
          // 429s are retried silently.
          return null;
        default:
          checkinError(
              "HTTP "
                  + ex.getResponse().getStatus()
                  + " error: Unable to check in with Wavefront! "
                  + multicastingTenantProxyConfig.getWFServer()
                  + ": "
                  + Throwables.getRootCause(ex).getMessage());
      }
      return Maps.newHashMap(); // return empty configuration to prevent checking in every 1s
    } catch (ProcessingException ex) {
      Throwable rootCause = Throwables.getRootCause(ex);
      if (rootCause instanceof UnknownHostException) {
        checkinError(
            "Unknown host: "
                + multicastingTenantProxyConfig.getWFServer()
                + ". Please verify your DNS and network settings!");
        return null;
      }
      if (rootCause instanceof ConnectException) {
        checkinError(
            "Unable to connect to "
                + multicastingTenantProxyConfig.getWFServer()
                + ": "
                + rootCause.getMessage()
                + " Please verify your network/firewall settings!");
        return null;
      }
      if (rootCause instanceof SocketTimeoutException) {
        checkinError(
            "Unable to check in with "
                + multicastingTenantProxyConfig.getWFServer()
                + ": "
                + rootCause.getMessage()
                + " Please verify your network/firewall settings!");
        return null;
      }
      checkinError(
          "Request processing error: Unable to retrieve proxy configuration! "
              + multicastingTenantProxyConfig.getWFServer()
              + ": "
              + rootCause);
      return null;
    } catch (Exception ex) {
      checkinError(
          "Unable to retrieve proxy configuration from remote server! "
              + multicastingTenantProxyConfig.getWFServer()
              + ": "
              + Throwables.getRootCause(ex));
      return null;
    } finally {
      synchronized (executor) {
        // if check-in process failed (agentMetricsWorkingCopy is not null) and agent
        // metrics have
        // not been updated yet, restore last known set of agent metrics to be retried
        if (agentMetricsWorkingCopy != null && agentMetrics == null) {
          agentMetrics = agentMetricsWorkingCopy;
        }
      }
    }
    if (configurationList.get(APIContainer.CENTRAL_TENANT_NAME).currentTime != null) {
      Clock.set(configurationList.get(APIContainer.CENTRAL_TENANT_NAME).currentTime);
    }

    // Always update the log server url / token in case they've changed
    String logServerIngestionURL =
        configurationList.get(APIContainer.CENTRAL_TENANT_NAME).getLogServerEndpointUrl();
    String logServerIngestionToken =
        configurationList.get(APIContainer.CENTRAL_TENANT_NAME).getLogServerToken();

    // MONIT-33770 - For converged CSP tenants, a user needs to provide vRLIC Ingestion token &
    // URL as proxy configuration.
    String WARNING_MSG = "Missing either logServerIngestionToken/logServerIngestionURL or both.";
    if (StringUtils.isBlank(logServerIngestionURL)
        && StringUtils.isBlank(logServerIngestionToken)) {
      ValidationConfiguration validationConfiguration =
          configurationList.get(APIContainer.CENTRAL_TENANT_NAME).getValidationConfiguration();
      if (validationConfiguration != null
          && validationConfiguration.enableHyperlogsConvergedCsp()) {
        proxyConfig.setEnableHyperlogsConvergedCsp(true);
        logServerIngestionURL = proxyConfig.getLogServerIngestionURL();
        logServerIngestionToken = proxyConfig.getLogServerIngestionToken();
        if (StringUtils.isBlank(logServerIngestionURL)
            || StringUtils.isBlank(logServerIngestionToken)) {
          proxyConfig.setReceivedLogServerDetails(false);
          logger.severe(
              WARNING_MSG
                  + " To ingest logs to the log server, please provide "
                  + "logServerIngestionToken & logServerIngestionURL in the proxy configuration.");
        }
      }
    } else if (StringUtils.isBlank(logServerIngestionURL)
        || StringUtils.isBlank(logServerIngestionToken)) {
      logger.severe(
          WARNING_MSG
              + " Proxy will not be ingesting data to the log server as it did "
              + "not receive at least one of the values during check-in.");
    }

    apiContainer.updateLogServerEndpointURLandToken(logServerIngestionURL, logServerIngestionToken);

    return configurationList;
  }

  @VisibleForTesting
  void updateConfiguration() {
    try {
      Map<String, AgentConfiguration> configList = checkin();
      if (configList != null && !configList.isEmpty()) {
        AgentConfiguration config;
        for (Map.Entry<String, AgentConfiguration> configEntry : configList.entrySet()) {
          config = configEntry.getValue();
          // For shutdown the proxy / truncate queue, only check the central tenant's flag
          if (config == null) {
            continue;
          }
          if (configEntry.getKey().equals(APIContainer.CENTRAL_TENANT_NAME)) {
            if (logger.isLoggable(Level.FINE)) {
              logger.fine("Server configuration getShutOffAgents: " + config.getShutOffAgents());
              logger.fine("Server configuration isTruncateQueue: " + config.isTruncateQueue());
            }
            if (config.getShutOffAgents()) {
              logger.severe(
                  firstNonNull(
                      config.getShutOffMessage(),
                      "Shutting down: Server side flag indicating proxy has to shut down."));
              shutdownHook.run();
            } else if (config.isTruncateQueue()) {
              logger.severe(
                  "Truncating queue: Server side flag indicating proxy queue has to be truncated.");
              truncateBacklog.run();
            }
          }
          agentConfigurationConsumer.accept(configEntry.getKey(), config);

          // Check if preprocessor rules were set on server side
          String checkPreprocessorRules = config.getPreprocessorRules();
          if (checkPreprocessorRules != null && !checkPreprocessorRules.isEmpty()) {
            AgentConfiguration finalConfig = config;
            logger.log(Level.INFO, () -> String.format("New preprocessor rules detected during checkin. Setting new preprocessor rule %s",
                    (finalConfig.getPreprocessorRulesId() != null && !finalConfig.getPreprocessorRulesId().isEmpty()) ? finalConfig.getPreprocessorRulesId() : ""));
            // future implementation, can send timestamp through AgentConfig and skip reloading if rule unchanged
            isRulesSetInFE.set(true);
            // indicates will need to sendPreprocessorRules()
            preprocessorRulesNeedUpdate.set(true);
          } else {
            // was previously reading from BE
            if (isRulesSetInFE.get()) {
              if (proxyConfig.getPreprocessorConfigFile() == null || proxyConfig.getPreprocessorConfigFile().isEmpty()) {
                logger.info("No preprocessor rules detected during checkin, and no rules file found.");
              } else {
                logger.log(Level.INFO, () -> String.format("Reverting back to reading rules from file %s", proxyConfig.getPreprocessorConfigFile()));
              }
              // indicates that previously read from BE, now switching back to reading from file.
              isRulesSetInFE.set(false);
              preprocessorRulesNeedUpdate.set(true);
            }
          }
          // will always send to BE in order to update Agent with latest rule
          sendPreprocessorRules(config);
        }
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Exception occurred during configuration update", e);
    }
  }

  @VisibleForTesting
  void updateProxyMetrics() {
    try {
      Map<String, String> pointTags = new HashMap<>(proxyConfig.getAgentMetricsPointTags());
      pointTags.put("processId", ID);
      pointTags.put("hostname", proxyConfig.getHostname());
      synchronized (executor) {
        agentMetrics =
            JsonMetricsGenerator.generateJsonMetrics(
                Metrics.defaultRegistry(), true, true, true, pointTags, null);
        retries.set(0);
      }
    } catch (Exception ex) {
      logger.log(Level.SEVERE, "Could not generate proxy metrics", ex);
    }
  }

  private void checkinError(String errMsg) {
    if (successfulCheckIns.get() == 0) logger.severe(Strings.repeat("*", errMsg.length()));
    logger.severe(errMsg);
    if (successfulCheckIns.get() == 0) logger.severe(Strings.repeat("*", errMsg.length()));
  }
}
