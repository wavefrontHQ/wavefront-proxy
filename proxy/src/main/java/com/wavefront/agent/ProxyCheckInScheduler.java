package com.wavefront.agent;

import static com.wavefront.common.Utils.getBuildVersion;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.api.agent.AgentConfiguration;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers the proxy with the back-end, sets up regular "check-ins" (every minute), transmits
 * proxy metrics to the back-end.
 */
public class ProxyCheckInScheduler {
  private static final Logger logger =
      LoggerFactory.getLogger(ProxyCheckInScheduler.class.getCanonicalName());
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
  private String hostname;
  private final AtomicInteger retries = new AtomicInteger(0);
  private final AtomicLong successfulCheckIns = new AtomicLong(0);
  /** Executors for support tasks. */
  private final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(2, new NamedThreadFactory("proxy-configuration"));

  private String serverEndpointUrl = null;
  private volatile JsonNode agentMetrics;
  private boolean retryImmediately = false;

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
    if (configList == null && retryImmediately) {
      // immediately retry check-ins if we need to re-attempt
      // due to changing the server endpoint URL
      updateProxyMetrics();
      configList = checkin();
    }
    if (configList != null && !configList.isEmpty()) {
      logger.info("initial configuration is available, setting up proxy");
      for (Map.Entry<String, AgentConfiguration> configEntry : configList.entrySet()) {
        agentConfigurationConsumer.accept(configEntry.getKey(), configEntry.getValue());
        successfulCheckIns.incrementAndGet();
      }
    }
    hostname = proxyConfig.getHostname();
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
    Map<String, Map<String, String>> multicastingTenantList =
        proxyConfig.getMulticastingTenantList();
    // Initialize tenantName and multicastingTenantProxyConfig here to track current
    // checking
    // tenant for better exception handling message
    String tenantName = APIContainer.CENTRAL_TENANT_NAME;
    Map<String, String> multicastingTenantProxyConfig =
        multicastingTenantList.get(APIContainer.CENTRAL_TENANT_NAME);
    try {
      AgentConfiguration multicastingConfig;
      for (Map.Entry<String, Map<String, String>> multicastingTenantEntry :
          multicastingTenantList.entrySet()) {
        tenantName = multicastingTenantEntry.getKey();
        multicastingTenantProxyConfig = multicastingTenantEntry.getValue();
        logger.info(
            "Checking in tenants: " + multicastingTenantProxyConfig.get(APIContainer.API_SERVER));
        multicastingConfig =
            apiContainer
                .getProxyV2APIForTenant(tenantName)
                .proxyCheckin(
                    proxyId,
                    "Bearer " + multicastingTenantProxyConfig.get(APIContainer.API_TOKEN),
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
          String serverUrl =
              multicastingTenantProxyConfig.get(APIContainer.API_SERVER).replaceAll("/$", "");
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
                  ? "Current setting: " + multicastingTenantProxyConfig.get(APIContainer.API_SERVER)
                  : "Server endpoint URLs normally end with '/api/'. Current setting: "
                      + multicastingTenantProxyConfig.get(APIContainer.API_SERVER);
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
                  + multicastingTenantProxyConfig.get(APIContainer.API_SERVER)
                  + ": "
                  + Throwables.getRootCause(ex).getMessage());
      }
      return Maps.newHashMap(); // return empty configuration to prevent checking in every 1s
    } catch (ProcessingException ex) {
      Throwable rootCause = Throwables.getRootCause(ex);
      if (rootCause instanceof UnknownHostException) {
        checkinError(
            "Unknown host: "
                + multicastingTenantProxyConfig.get(APIContainer.API_SERVER)
                + ". Please verify your DNS and network settings!");
        return null;
      }
      if (rootCause instanceof ConnectException) {
        checkinError(
            "Unable to connect to "
                + multicastingTenantProxyConfig.get(APIContainer.API_SERVER)
                + ": "
                + rootCause.getMessage()
                + " Please verify your network/firewall settings!");
        return null;
      }
      if (rootCause instanceof SocketTimeoutException) {
        checkinError(
            "Unable to check in with "
                + multicastingTenantProxyConfig.get(APIContainer.API_SERVER)
                + ": "
                + rootCause.getMessage()
                + " Please verify your network/firewall settings!");
        return null;
      }
      checkinError(
          "Request processing error: Unable to retrieve proxy configuration! "
              + multicastingTenantProxyConfig.get(APIContainer.API_SERVER)
              + ": "
              + rootCause);
      return null;
    } catch (Exception ex) {
      checkinError(
          "Unable to retrieve proxy configuration from remote server! "
              + multicastingTenantProxyConfig.get(APIContainer.API_SERVER)
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
    apiContainer.updateLogServerEndpointURLandToken(
        configurationList.get(APIContainer.CENTRAL_TENANT_NAME).getLogServerEndpointUrl(),
        configurationList.get(APIContainer.CENTRAL_TENANT_NAME).getLogServerToken());
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
            if (logger.isDebugEnabled()) {
              logger.info("Server configuration getShutOffAgents: " + config.getShutOffAgents());
              logger.info("Server configuration isTruncateQueue: " + config.isTruncateQueue());
            }
            if (config.getShutOffAgents()) {
              logger.warn(
                  firstNonNull(
                      config.getShutOffMessage(),
                      "Shutting down: Server side flag indicating proxy has to shut down."));
              shutdownHook.run();
            } else if (config.isTruncateQueue()) {
              logger.warn(
                  "Truncating queue: Server side flag indicating proxy queue has to be truncated.");
              truncateBacklog.run();
            }
          }
          agentConfigurationConsumer.accept(configEntry.getKey(), config);
        }
      }
    } catch (Exception e) {
      logger.error("Exception occurred during configuration update", e);
    }
  }

  @VisibleForTesting
  void updateProxyMetrics() {
    try {
      Map<String, String> pointTags = new HashMap<>(proxyConfig.getAgentMetricsPointTags());
      pointTags.put("processId", ID);
      // MONIT-27856 Adds real hostname (fqdn if possible) as internal metric tag
      pointTags.put("hostname", hostname);
      synchronized (executor) {
        agentMetrics =
            JsonMetricsGenerator.generateJsonMetrics(
                Metrics.defaultRegistry(), true, true, true, pointTags, null);
        retries.set(0);
      }
    } catch (Exception ex) {
      logger.error("Could not generate proxy metrics", ex);
    }
  }

  private void checkinError(String errMsg) {
    if (successfulCheckIns.get() == 0) logger.error(Strings.repeat("*", errMsg.length()));
    logger.error(errMsg);
    if (successfulCheckIns.get() == 0) logger.error(Strings.repeat("*", errMsg.length()));
  }
}
