package com.wavefront.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.metrics.JsonMetricsGenerator;
import com.yammer.metrics.Metrics;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ProcessingException;
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
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.wavefront.common.Utils.getBuildVersion;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * Registers the proxy with the back-end, sets up regular "check-ins" (every minute),
 * transmits proxy metrics to the back-end.
 *
 * @author vasily@wavefront.com
 */
public class ProxyCheckInScheduler {
  private static final Logger logger = Logger.getLogger("proxy");
  private static final int MAX_CHECKIN_ATTEMPTS = 5;

  /**
   * A unique value (a random hexadecimal string), assigned at proxy start-up, to be reported
   * with all ~proxy metrics as a "processId" point tag to prevent potential ~proxy metrics
   * collisions caused by users spinning up multiple proxies with duplicate names.
   */
  private static final String ID = Integer.toHexString((int) (Math.random() * Integer.MAX_VALUE));

  private final UUID proxyId;
  private final ProxyConfig proxyConfig;
  private final APIContainer apiContainer;
  private final Consumer<AgentConfiguration> agentConfigurationConsumer;
  private final Runnable shutdownHook;

  private String serverEndpointUrl = null;
  private volatile JsonNode agentMetrics;
  private final AtomicInteger retries = new AtomicInteger(0);
  private final AtomicLong successfulCheckIns = new AtomicLong(0);
  private boolean retryImmediately = false;

  /**
   * Executors for support tasks.
   */
  private final ScheduledExecutorService executor = Executors.
      newScheduledThreadPool(2, new NamedThreadFactory("proxy-configuration"));

  /**
   * @param proxyId                    Proxy UUID.
   * @param proxyConfig                Proxy settings.
   * @param apiContainer               API container object.
   * @param agentConfigurationConsumer Configuration processor, invoked after each
   *                                   successful configuration fetch.
   * @param shutdownHook               Invoked when proxy receives a shutdown directive
   *                                   from the back-end.
   */
  public ProxyCheckInScheduler(UUID proxyId,
                               ProxyConfig proxyConfig,
                               APIContainer apiContainer,
                               Consumer<AgentConfiguration> agentConfigurationConsumer,
                               Runnable shutdownHook) {
    this.proxyId = proxyId;
    this.proxyConfig = proxyConfig;
    this.apiContainer = apiContainer;
    this.agentConfigurationConsumer = agentConfigurationConsumer;
    this.shutdownHook = shutdownHook;
    updateProxyMetrics();
    AgentConfiguration config = checkin();
    if (config == null && retryImmediately) {
      // immediately retry check-ins if we need to re-attempt
      // due to changing the server endpoint URL
      updateProxyMetrics();
      config = checkin();
    }
    if (config != null) {
      logger.info("initial configuration is available, setting up proxy");
      agentConfigurationConsumer.accept(config);
      successfulCheckIns.incrementAndGet();
    }
  }

  /**
   * Set up and schedule regular check-ins.
   */
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

  /**
   * Stops regular check-ins.
   */
  public void shutdown() {
    executor.shutdown();
  }

  /**
   * Perform agent check-in and fetch configuration of the daemon from remote server.
   *
   * @return Fetched configuration. {@code null} if the configuration is invalid.
   */
  private AgentConfiguration checkin() {
    AgentConfiguration newConfig;
    JsonNode agentMetricsWorkingCopy;
    synchronized (executor) {
      if (agentMetrics == null) return null;
      agentMetricsWorkingCopy = agentMetrics;
      agentMetrics = null;
      if (retries.incrementAndGet() > MAX_CHECKIN_ATTEMPTS) return null;
    }
    logger.info("Checking in: " + firstNonNull(serverEndpointUrl, proxyConfig.getServer()));
    try {
      newConfig = apiContainer.getProxyV2API().proxyCheckin(proxyId,
          "Bearer " + proxyConfig.getToken(), proxyConfig.getHostname(), getBuildVersion(),
          System.currentTimeMillis(), agentMetricsWorkingCopy, proxyConfig.isEphemeral());
      agentMetricsWorkingCopy = null;
    } catch (ClientErrorException ex) {
      agentMetricsWorkingCopy = null;
      switch (ex.getResponse().getStatus()) {
        case 401:
          checkinError("HTTP 401 Unauthorized: Please verify that your server and token settings" +
              " are correct and that the token has Proxy Management permission!");
          if (successfulCheckIns.get() == 0) {
            throw new RuntimeException("Aborting start-up");
          }
          break;
        case 403:
          checkinError("HTTP 403 Forbidden: Please verify that your token has Proxy Management " +
              "permission!");
          if (successfulCheckIns.get() == 0) {
            throw new RuntimeException("Aborting start-up");
          }
          break;
        case 404:
        case 405:
          String serverUrl = proxyConfig.getServer().replaceAll("/$", "");
          if (successfulCheckIns.get() == 0 && !retryImmediately && !serverUrl.endsWith("/api")) {
            this.serverEndpointUrl = serverUrl + "/api/";
            checkinError("Possible server endpoint misconfiguration detected, attempting to use " +
                serverEndpointUrl);
            apiContainer.updateServerEndpointURL(serverEndpointUrl);
            retryImmediately = true;
            return null;
          }
          String secondaryMessage = serverUrl.endsWith("/api") ?
              "Current setting: " + proxyConfig.getServer() :
              "Server endpoint URLs normally end with '/api/'. Current setting: " +
                  proxyConfig.getServer();
          checkinError("HTTP " + ex.getResponse().getStatus() + ": Misconfiguration detected, " +
              "please verify that your server setting is correct. " + secondaryMessage);
          if (successfulCheckIns.get() == 0) {
            throw new RuntimeException("Aborting start-up");
          }
          break;
        case 407:
          checkinError("HTTP 407 Proxy Authentication Required: Please verify that " +
              "proxyUser and proxyPassword settings are correct and make sure your HTTP proxy" +
              " is not rate limiting!");
          if (successfulCheckIns.get() == 0) {
            throw new RuntimeException("Aborting start-up");
          }
          break;
        case 429:
          // 429s are retried silently.
          return null;
        default:
          checkinError("HTTP " + ex.getResponse().getStatus() +
              " error: Unable to check in with Wavefront! " + proxyConfig.getServer() + ": " +
              Throwables.getRootCause(ex).getMessage());
      }
      return new AgentConfiguration(); // return empty configuration to prevent checking in every 1s
    } catch (ProcessingException ex) {
      Throwable rootCause = Throwables.getRootCause(ex);
      if (rootCause instanceof UnknownHostException) {
        checkinError("Unknown host: " + proxyConfig.getServer() +
            ". Please verify your DNS and network settings!");
        return null;
      }
      if (rootCause instanceof ConnectException) {
        checkinError("Unable to connect to " + proxyConfig.getServer() + ": " +
            rootCause.getMessage() + " Please verify your network/firewall settings!");
        return null;
      }
      if (rootCause instanceof SocketTimeoutException) {
        checkinError("Unable to check in with " + proxyConfig.getServer() + ": " +
            rootCause.getMessage() + " Please verify your network/firewall settings!");
        return null;
      }
      checkinError("Request processing error: Unable to retrieve proxy configuration! " +
          proxyConfig.getServer() + ": " + rootCause);
      return null;
    } catch (Exception ex) {
      checkinError("Unable to retrieve proxy configuration from remote server! " +
          proxyConfig.getServer() + ": " + Throwables.getRootCause(ex));
      return null;
    } finally {
      synchronized (executor) {
        // if check-in process failed (agentMetricsWorkingCopy is not null) and agent metrics have
        // not been updated yet, restore last known set of agent metrics to be retried
        if (agentMetricsWorkingCopy != null && agentMetrics == null) {
          agentMetrics = agentMetricsWorkingCopy;
        }
      }
    }
    if (newConfig.currentTime != null) {
      Clock.set(newConfig.currentTime);
    }
    return newConfig;
  }

  @VisibleForTesting
  void updateConfiguration() {
    try {
      AgentConfiguration config = checkin();
      if (config != null) {
        if (config.getShutOffAgents()) {
          logger.severe(firstNonNull(config.getShutOffMessage(),
              "Shutting down: Server side flag indicating proxy has to shut down."));
          shutdownHook.run();
        } else {
          agentConfigurationConsumer.accept(config);
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
      synchronized (executor) {
        agentMetrics = JsonMetricsGenerator.generateJsonMetrics(Metrics.defaultRegistry(),
            true, true, true, pointTags, null);
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
