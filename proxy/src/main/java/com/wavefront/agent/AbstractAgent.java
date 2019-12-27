package com.wavefront.agent;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.auth.TokenAuthenticatorBuilder;
import com.wavefront.agent.channel.DisableGZIPEncodingInterceptor;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.config.LogsIngestionConfig;
import com.wavefront.agent.config.ProxyConfig;
import com.wavefront.agent.data.EntityWrapper;
import com.wavefront.agent.data.ProxyRuntimeProperties;
import com.wavefront.agent.logsharvesting.InteractiveLogsTester;
import com.wavefront.agent.preprocessor.PointLineBlacklistRegexFilter;
import com.wavefront.agent.preprocessor.PointLineWhitelistRegexFilter;
import com.wavefront.agent.preprocessor.PreprocessorConfigManager;
import com.wavefront.agent.preprocessor.PreprocessorRuleMetrics;
import com.wavefront.api.EventAPI;
import com.wavefront.api.ProxyV2API;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.wavefront.metrics.JsonMetricsGenerator;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.jboss.resteasy.client.jaxrs.ClientHttpEngine;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.client.jaxrs.engines.ApacheHttpClient4Engine;
import org.jboss.resteasy.client.jaxrs.internal.LocalResteasyProviderFactory;
import org.jboss.resteasy.plugins.interceptors.encoding.AcceptEncodingGZIPFilter;
import org.jboss.resteasy.plugins.interceptors.encoding.GZIPDecodingInterceptor;
import org.jboss.resteasy.plugins.interceptors.encoding.GZIPEncodingInterceptor;
import org.jboss.resteasy.plugins.providers.jackson.ResteasyJackson2Provider;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import javax.annotation.Nullable;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.ClientRequestFilter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.Authenticator;
import java.net.ConnectException;
import java.net.PasswordAuthentication;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.wavefront.agent.ProxyUtil.getProcessId;
import static com.wavefront.agent.Utils.getBuildVersion;
import static com.wavefront.agent.data.ProxyRuntimeProperties.DEFAULT_MIN_SPLIT_BATCH_SIZE;
import static com.wavefront.agent.config.ReportableConfig.reportSettingAsGauge;

/**
 * Agent that runs remotely on a server collecting metrics.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public abstract class AbstractAgent {

  protected static final Logger logger = Logger.getLogger("proxy");
  final Counter activeListeners = Metrics.newCounter(ExpectedAgentMetric.ACTIVE_LISTENERS.metricName);

  private static final ObjectMapper JSON_PARSER = new ObjectMapper();

  /**
   * A set of commandline parameters to hide when echoing command line arguments
   */
  protected static final Set<String> PARAMETERS_TO_HIDE = ImmutableSet.of("-t", "--token",
      "--proxyPassword");

  protected final ProxyConfig proxyConfig = new ProxyConfig();
  protected APIContainer apiContainer;
  protected final List<ExecutorService> managedExecutors = new ArrayList<>();
  protected final List<Runnable> shutdownTasks = new ArrayList<>();
  protected PreprocessorConfigManager preprocessors = new PreprocessorConfigManager();
  protected ValidationConfiguration validationConfiguration = null;
  protected ProxyRuntimeProperties runtimeProperties = null;
  protected EntityWrapper entityWrapper = null;
  protected TokenAuthenticator tokenAuthenticator = TokenAuthenticatorBuilder.create().build();
  protected JsonNode agentMetrics;
  protected long agentMetricsCaptureTs;
  protected volatile boolean hadSuccessfulCheckin = false;
  protected volatile boolean retryCheckin = false;
  protected final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  protected String serverEndpointUrl = null;

  /**
   * A unique process ID value (PID, when available, or a random hexadecimal string), assigned
   * at proxy start-up, to be reported with all ~proxy metrics as a "processId" point tag to
   * prevent potential ~proxy metrics collisions caused by users spinning up multiple proxies
   * with duplicate names.
   */
  protected final String processId = getProcessId();

  protected final boolean localAgent;
  protected final boolean pushAgent;

  // Will be updated inside processConfiguration method and the new configuration is periodically
  // loaded from the server by invoking agentAPI.checkin
  protected final AtomicBoolean histogramDisabled = new AtomicBoolean(false);
  protected final AtomicBoolean traceDisabled = new AtomicBoolean(false);
  protected final AtomicBoolean spanLogsDisabled = new AtomicBoolean(false);

  /**
   * Executors for support tasks.
   */
  private final ScheduledExecutorService agentConfigurationExecutor = Executors.
      newScheduledThreadPool(2, new NamedThreadFactory("proxy-configuration"));
  protected UUID agentId;
  private final Runnable updateConfiguration = () -> {
    boolean doShutDown = false;
    try {
      AgentConfiguration config = checkin();
      if (config != null) {
        processConfiguration(config);
        doShutDown = config.getShutOffAgents();
        if (config.getValidationConfiguration() != null) {
          this.validationConfiguration = config.getValidationConfiguration();
        }
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Exception occurred during configuration update", e);
    } finally {
      if (doShutDown) {
        logger.warning("Shutting down: Server side flag indicating proxy has to shut down.");
        System.exit(1);
      }
    }
  };

  private final Runnable updateAgentMetrics = () -> {
    try {
      Map<String, String> pointTags = new HashMap<>(proxyConfig.getAgentMetricsPointTags());
      pointTags.put("processId", processId);
      synchronized (agentConfigurationExecutor) {
        agentMetricsCaptureTs = System.currentTimeMillis();
        agentMetrics = JsonMetricsGenerator.generateJsonMetrics(Metrics.defaultRegistry(),
            true, true, true, pointTags, null);
      }
    } catch (Exception ex) {
      logger.log(Level.SEVERE, "Could not generate proxy metrics", ex);
    }
  };

  public AbstractAgent() {
    this(false, false);
  }

  public AbstractAgent(boolean localAgent, boolean pushAgent) {
    this.pushAgent = pushAgent;
    this.localAgent = localAgent;
    Metrics.newGauge(ExpectedAgentMetric.BUFFER_BYTES_LEFT.metricName,
        new Gauge<Long>() {
          @Override
          public Long value() {
            try {
              File bufferDirectory = new File(proxyConfig.getBufferFile()).getAbsoluteFile();
              while (bufferDirectory != null && bufferDirectory.getUsableSpace() == 0) {
                bufferDirectory = bufferDirectory.getParentFile();
              }
              if (bufferDirectory != null) {
                return bufferDirectory.getUsableSpace();
              }
            } catch (Throwable t) {
              logger.warning("cannot compute remaining space in buffer file partition: " + t);
            }
            return null;
          }
        }
    );
  }

  private void addPreprocessorFilters(String commaDelimitedPorts, String whitelist,
                                      String blacklist) {
    if (commaDelimitedPorts != null && (whitelist != null || blacklist != null)) {
      for (String strPort : Splitter.on(",").omitEmptyStrings().trimResults().split(commaDelimitedPorts)) {
        PreprocessorRuleMetrics ruleMetrics = new PreprocessorRuleMetrics(
            Metrics.newCounter(new TaggedMetricName("validationRegex", "points-rejected", "port", strPort)),
            Metrics.newCounter(new TaggedMetricName("validationRegex", "cpu-nanos", "port", strPort)),
            Metrics.newCounter(new TaggedMetricName("validationRegex", "points-checked", "port", strPort))
        );
        if (blacklist != null) {
          preprocessors.getSystemPreprocessor(strPort).forPointLine().addFilter(
              new PointLineBlacklistRegexFilter(proxyConfig.getBlacklistRegex(), ruleMetrics));
        }
        if (whitelist != null) {
          preprocessors.getSystemPreprocessor(strPort).forPointLine().addFilter(
              new PointLineWhitelistRegexFilter(whitelist, ruleMetrics));
        }
      }
    }
  }

  private void initPreprocessors() {
    try {
      preprocessors = new PreprocessorConfigManager(proxyConfig.getPreprocessorConfigFile());
    } catch (FileNotFoundException ex) {
      throw new RuntimeException("Unable to load preprocessor rules - file does not exist: " +
          proxyConfig.getPreprocessorConfigFile());
    }
    if (proxyConfig.getPreprocessorConfigFile() != null) {
      logger.info("Preprocessor configuration loaded from " +
          proxyConfig.getPreprocessorConfigFile());
    }

    // convert blacklist/whitelist fields to filters for full backwards compatibility
    // blacklistRegex and whitelistRegex are applied to pushListenerPorts, graphitePorts and picklePorts
    String allPorts = StringUtils.join(new String[]{
        ObjectUtils.firstNonNull(proxyConfig.getPushListenerPorts(), ""),
        ObjectUtils.firstNonNull(proxyConfig.getGraphitePorts(), ""),
        ObjectUtils.firstNonNull(proxyConfig.getPicklePorts(), ""),
        ObjectUtils.firstNonNull(proxyConfig.getTraceListenerPorts(), "")
    }, ",");
    addPreprocessorFilters(allPorts, proxyConfig.getWhitelistRegex(),
        proxyConfig.getBlacklistRegex());
    // opentsdbBlacklistRegex and opentsdbWhitelistRegex are applied to opentsdbPorts only
    addPreprocessorFilters(proxyConfig.getOpentsdbPorts(), proxyConfig.getOpentsdbWhitelistRegex(),
        proxyConfig.getOpentsdbBlacklistRegex());
  }

  // Returns null on any exception, and logs the exception.
  protected LogsIngestionConfig loadLogsIngestionConfig() {
    try {
      if (proxyConfig.getLogsIngestionConfigFile() == null) {
        return null;
      }
      ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
      return objectMapper.readValue(new File(proxyConfig.getLogsIngestionConfigFile()),
          LogsIngestionConfig.class);
    } catch (UnrecognizedPropertyException e) {
      logger.severe("Unable to load logs ingestion config: " + e.getMessage());
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Could not load logs ingestion config", e);
    }
    return null;
  }

  private void postProcessConfig() {
    runtimeProperties = ProxyRuntimeProperties.newBuilder().
        setSplitPushWhenRateLimited(proxyConfig.isSplitPushWhenRateLimited()).
        setRetryBackoffBaseSeconds(proxyConfig.getRetryBackoffBaseSeconds()).
        setPushRateLimitMaxBurstSeconds(proxyConfig.getPushRateLimitMaxBurstSeconds()).
        setPushFlushInterval(proxyConfig.getPushFlushInterval()).
        setItemsPerBatch(proxyConfig.getPushFlushMaxPoints()).
        setItemsPerBatchHistograms(proxyConfig.getPushFlushMaxHistograms()).
        setItemsPerBatchSourceTags(proxyConfig.getPushFlushMaxSourceTags()).
        setItemsPerBatchSpans(proxyConfig.getPushFlushMaxSpans()).
        setItemsPerBatchSpanLogs(proxyConfig.getPushFlushMaxSpanLogs()).
        setItemsPerBatchEvents(proxyConfig.getPushFlushMaxEvents()).
        setMinBatchSplitSize(DEFAULT_MIN_SPLIT_BATCH_SIZE).
        setMemoryBufferLimit(proxyConfig.getPushMemoryBufferLimit()).
        setMemoryBufferLimitSourceTags(16 * proxyConfig.getPushFlushMaxSourceTags()).
        setMemoryBufferLimitEvents(16 * proxyConfig.getPushFlushMaxEvents()).
        build();
    reportSettingAsGauge(runtimeProperties::getPushFlushInterval, "pushFlushInterval");
    reportSettingAsGauge(runtimeProperties::getItemsPerBatch, "pushFlushMaxPoints");
    reportSettingAsGauge(runtimeProperties::getItemsPerBatchHistograms, "pushFlushMaxHistograms");
    reportSettingAsGauge(runtimeProperties::getItemsPerBatchSourceTags, "pushFlushMaxSourceTags");
    reportSettingAsGauge(runtimeProperties::getItemsPerBatchSpans, "pushFlushMaxSpans");
    reportSettingAsGauge(runtimeProperties::getItemsPerBatchSpanLogs, "pushFlushMaxSpanLogs");
    reportSettingAsGauge(runtimeProperties::getItemsPerBatchEvents, "pushFlushMaxEvents");
    reportSettingAsGauge(runtimeProperties::getRetryBackoffBaseSeconds, "retryBackoffBaseSeconds");
    reportSettingAsGauge(runtimeProperties::getMemoryBufferLimit, "pushMemoryBufferLimit");

    Logger retryLogger = Logger.getLogger("org.apache.http.impl.execchain.RetryExec");
    if (retryLogger.getLevel() == Level.INFO) retryLogger.setLevel(Level.WARNING);

    if (StringUtils.isBlank(proxyConfig.getHostname().trim())) {
      logger.severe("hostname cannot be blank! Please correct your configuration settings.");
      System.exit(1);
    }
  }

  @VisibleForTesting
  void parseArguments(String[] args) {
    // read build information and print version.
    String versionStr = "Wavefront Proxy version " + getBuildVersion();
    JCommander jCommander = JCommander.newBuilder().
        programName(this.getClass().getCanonicalName()).
        addObject(proxyConfig).
        allowParameterOverwriting(true).
        build();
    try {
      jCommander.parse(args);

      if (proxyConfig.isVersion()) {
        System.out.println(versionStr);
        System.exit(0);
      }
      if (proxyConfig.isHelp()) {
        System.out.println(versionStr);
        jCommander.usage();
        System.exit(0);
      }
      logger.info(versionStr);
      logger.info("Arguments: " + IntStream.range(0, args.length).
          mapToObj(i -> (i > 0 && PARAMETERS_TO_HIDE.contains(args[i - 1])) ? "<HIDDEN>" : args[i]).
          collect(Collectors.joining(" ")));
      proxyConfig.verifyAndInit();
    } catch (ParameterException e) {
      logger.severe("Parameter exception: " + e.getMessage());
      System.exit(1);
    } catch (ConfigurationException e) {
      logger.severe("Configuration exception: " + e.getMessage());
      System.exit(1);
    }
  }

  /**
   * Entry-point for the application.
   *
   * @param args Command-line parameters passed on to JCommander to configure the daemon.
   */
  public void start(String[] args) {
    try {

      /* ------------------------------------------------------------------------------------
       * Configuration Setup.
       * ------------------------------------------------------------------------------------ */

      // Parse commandline arguments and load configuration file
      parseArguments(args);
      postProcessConfig();
      initPreprocessors();
      configureTokenAuthenticator();

      managedExecutors.add(agentConfigurationExecutor);

      // Conditionally enter an interactive debugging session for logsIngestionConfig.yaml
      if (proxyConfig.isTestLogs()) {
        InteractiveLogsTester interactiveLogsTester = new InteractiveLogsTester(
            this::loadLogsIngestionConfig, proxyConfig.getPrefix());
        logger.info("Reading line-by-line sample log messages from STDIN");
        //noinspection StatementWithEmptyBody
        while (interactiveLogsTester.interactiveTest()) {
          // empty
        }
        System.exit(0);
      }

      // 2. Read or create the unique Id for the daemon running on this machine.
      if (proxyConfig.isEphemeral()) {
        agentId = UUID.randomUUID(); // don't need to store one
        logger.info("Ephemeral proxy id created: " + agentId);
      } else {
        agentId = ProxyUtil.getOrCreateProxyId(proxyConfig.getIdFile());
      }

      configureHttpProxy();

      // Setup queueing.
      apiContainer = new APIContainer(createService(proxyConfig.getServer(), ProxyV2API.class),
          createService(proxyConfig.getServer(), SourceTagAPI.class),
          createService(proxyConfig.getServer(), EventAPI.class));
      //setupQueueing(service);

      // Perform initial proxy check-in and schedule regular check-ins (once a minute)
      setupCheckins();

      // Start the listening endpoints
      startListeners();

      // set up OoM memory guard
      if (proxyConfig.getMemGuardFlushThreshold() > 0) {
        setupMemoryGuard((float) proxyConfig.getMemGuardFlushThreshold() / 100);
      }

      new Timer().schedule(
          new TimerTask() {
            @Override
            public void run() {
              // exit if no active listeners
              if (activeListeners.count() == 0) {
                logger.severe("**** All listener threads failed to start - there is already a " +
                    "running instance listening on configured ports,  or no listening ports " +
                    "configured!");
                logger.severe("Aborting start-up");
                System.exit(1);
              }

              Runtime.getRuntime().addShutdownHook(new Thread("proxy-shutdown-hook") {
                @Override
                public void run() {
                  shutdown();
                }
              });

              logger.info("setup complete");
            }
          },
          5000
      );
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Aborting start-up", t);
      System.exit(1);
    }
  }

  private void configureHttpProxy() {
    if (proxyConfig.getProxyHost() != null) {
      System.setProperty("http.proxyHost", proxyConfig.getProxyHost());
      System.setProperty("https.proxyHost", proxyConfig.getProxyHost());
      System.setProperty("http.proxyPort", String.valueOf(proxyConfig.getProxyPort()));
      System.setProperty("https.proxyPort", String.valueOf(proxyConfig.getProxyPort()));
    }
    if (proxyConfig.getProxyUser() != null && proxyConfig.getProxyPassword() != null) {
      Authenticator.setDefault(
          new Authenticator() {
            @Override
            public PasswordAuthentication getPasswordAuthentication() {
              if (getRequestorType() == RequestorType.PROXY) {
                return new PasswordAuthentication(proxyConfig.getProxyUser(),
                    proxyConfig.getProxyPassword().toCharArray());
              } else {
                return null;
              }
            }
          }
      );
    }
  }

  /**
   * Create RESTeasy proxies for remote calls via HTTP.
   */
  protected <T> T createService(String serverEndpointUrl, Class<T> apiClass) {
    ResteasyProviderFactory factory = new LocalResteasyProviderFactory(
        ResteasyProviderFactory.getInstance());
    factory.registerProvider(JsonNodeWriter.class);
    if (!factory.getClasses().contains(ResteasyJackson2Provider.class)) {
      factory.registerProvider(ResteasyJackson2Provider.class);
    }
    ClientHttpEngine httpEngine;
    HttpClient httpClient = HttpClientBuilder.create().
        useSystemProperties().
        setUserAgent(proxyConfig.getHttpUserAgent()).
        setMaxConnTotal(proxyConfig.getHttpMaxConnTotal()).
        setMaxConnPerRoute(proxyConfig.getHttpMaxConnPerRoute()).
        setConnectionTimeToLive(1, TimeUnit.MINUTES).
        setDefaultSocketConfig(
            SocketConfig.custom().
                setSoTimeout(proxyConfig.getHttpRequestTimeout()).build()).
        setSSLSocketFactory(new SSLConnectionSocketFactoryImpl(
            SSLConnectionSocketFactory.getSystemSocketFactory(),
            proxyConfig.getHttpRequestTimeout())).
        setRetryHandler(new DefaultHttpRequestRetryHandler(proxyConfig.getHttpAutoRetries(), true) {
          @Override
          protected boolean handleAsIdempotent(HttpRequest request) {
            // by default, retry all http calls (submissions are idempotent).
            return true;
          }
        }).
        setDefaultRequestConfig(
            RequestConfig.custom().
                setContentCompressionEnabled(true).
                setRedirectsEnabled(true).
                setConnectTimeout(proxyConfig.getHttpConnectTimeout()).
                setConnectionRequestTimeout(proxyConfig.getHttpConnectTimeout()).
                setSocketTimeout(proxyConfig.getHttpRequestTimeout()).build()).
        build();
    final ApacheHttpClient4Engine apacheHttpClient4Engine = new ApacheHttpClient4Engine(httpClient,
        true);
    // avoid using disk at all
    apacheHttpClient4Engine.setFileUploadInMemoryThresholdLimit(100);
    apacheHttpClient4Engine.setFileUploadMemoryUnit(ApacheHttpClient4Engine.MemoryUnit.MB);
    httpEngine = apacheHttpClient4Engine;
    ResteasyClient client = new ResteasyClientBuilder().
        httpEngine(httpEngine).
        providerFactory(factory).
        register(GZIPDecodingInterceptor.class).
        register(proxyConfig.isGzipCompression() ?
            GZIPEncodingInterceptor.class : DisableGZIPEncodingInterceptor.class).
        register(AcceptEncodingGZIPFilter.class).
        register((ClientRequestFilter) context -> {
          if (context.getUri().getPath().contains("/v2/wfproxy") ||
              context.getUri().getPath().contains("/v2/source") ||
              context.getUri().getPath().contains("/event")) {
            context.getHeaders().add("Authorization", "Bearer " + proxyConfig.getToken());
          }
        }).
        build();
    ResteasyWebTarget target = client.target(serverEndpointUrl);
    return target.proxy(apiClass);
  }

  private void checkinError(String errMsg, @Nullable String secondErrMsg) {
    if (hadSuccessfulCheckin) {
      logger.severe(errMsg + (secondErrMsg == null ? "" : " " + secondErrMsg));
    } else {
      logger.severe(Strings.repeat("*", errMsg.length()));
      logger.severe(errMsg);
      if (secondErrMsg != null) {
        logger.severe(secondErrMsg);
      }
      logger.severe(Strings.repeat("*", errMsg.length()));
    }
  }

  /**
   * Perform agent check-in and fetch configuration of the daemon from remote server.
   *
   * @return Fetched configuration. {@code null} if the configuration is invalid.
   */
  private AgentConfiguration checkin() {
    AgentConfiguration newConfig;
    JsonNode agentMetricsWorkingCopy;
    long agentMetricsCaptureTsWorkingCopy;
    synchronized(agentConfigurationExecutor) {
      if (agentMetrics == null) return null;
      agentMetricsWorkingCopy = agentMetrics;
      agentMetricsCaptureTsWorkingCopy = agentMetricsCaptureTs;
      agentMetrics = null;
    }
    logger.info("Checking in: " + ObjectUtils.firstNonNull(serverEndpointUrl,
        proxyConfig.getServer()));
    try {
      newConfig = apiContainer.getProxyV2API().proxyCheckin(agentId,
          "Bearer " + proxyConfig.getToken(), proxyConfig.getHostname(), getBuildVersion(),
          agentMetricsCaptureTsWorkingCopy, agentMetricsWorkingCopy, proxyConfig.isEphemeral());
      agentMetricsWorkingCopy = null;
    } catch (ClientErrorException ex) {
      agentMetricsWorkingCopy = null;
      switch (ex.getResponse().getStatus()) {
        case 401:
          checkinError("HTTP 401 Unauthorized: Please verify that your server and token settings",
              "are correct and that the token has Proxy Management permission!");
          break;
        case 403:
          checkinError("HTTP 403 Forbidden: Please verify that your token has Proxy Management " +
              "permission!", null);
          break;
        case 404:
        case 405:
          String serverUrl = proxyConfig.getServer().replaceAll("/$", "");
          if (!hadSuccessfulCheckin && !retryCheckin && !serverUrl.endsWith("/api")) {
            this.serverEndpointUrl = serverUrl + "/api/";
            checkinError("Possible server endpoint misconfiguration detected, attempting to use " +
                serverEndpointUrl, null);
            apiContainer.setProxyV2API(createService(serverEndpointUrl, ProxyV2API.class));
            apiContainer.setSourceTagAPI(createService(serverEndpointUrl, SourceTagAPI.class));
            apiContainer.setEventAPI(createService(serverEndpointUrl, EventAPI.class));
            retryCheckin = true;
            return null;
          }
          String secondaryMessage = serverUrl.endsWith("/api") ?
              "Current setting: " + proxyConfig.getServer() :
              "Server endpoint URLs normally end with '/api/'. Current setting: " +
                  proxyConfig.getServer();
          checkinError("HTTP " + ex.getResponse().getStatus() + ": Misconfiguration detected, " +
              "please verify that your server setting is correct", secondaryMessage);
          if (!hadSuccessfulCheckin) {
            logger.warning("Aborting start-up");
            System.exit(-5);
          }
          break;
        case 407:
          checkinError("HTTP 407 Proxy Authentication Required: Please verify that " +
                  "proxyUser and proxyPassword",
              "settings are correct and make sure your HTTP proxy is not rate limiting!");
          break;
        default:
          checkinError("HTTP " + ex.getResponse().getStatus() +
                  " error: Unable to check in with Wavefront!",
              proxyConfig.getServer() + ": " + Throwables.getRootCause(ex).getMessage());
      }
      return new AgentConfiguration(); // return empty configuration to prevent checking in every 1s
    } catch (ProcessingException ex) {
      Throwable rootCause = Throwables.getRootCause(ex);
      if (rootCause instanceof UnknownHostException) {
        checkinError("Unknown host: " + proxyConfig.getServer() +
            ". Please verify your DNS and network settings!", null);
        return null;
      }
      if (rootCause instanceof ConnectException ||
          rootCause instanceof SocketTimeoutException) {
        checkinError("Unable to connect to " + proxyConfig.getServer() + ": " + rootCause.getMessage(),
            "Please verify your network/firewall settings!");
        return null;
      }
      checkinError("Request processing error: Unable to retrieve proxy configuration!",
          proxyConfig.getServer() + ": " + rootCause);
      return null;
    } catch (Exception ex) {
      checkinError("Unable to retrieve proxy configuration from remote server!",
          proxyConfig.getServer() + ": " + Throwables.getRootCause(ex));
      return null;
    } finally {
      synchronized(agentConfigurationExecutor) {
        // if check-in process failed (agentMetricsWorkingCopy is not null) and agent metrics have
        // not been updated yet, restore last known set of agent metrics to be retried
        if (agentMetricsWorkingCopy != null && agentMetrics == null) {
          agentMetrics = agentMetricsWorkingCopy;
        }
      }
    }
    try {
      if (newConfig.currentTime != null) {
        Clock.set(newConfig.currentTime);
      }
      newConfig.validate(localAgent);
    } catch (Exception ex) {
      logger.log(Level.WARNING, "configuration file read from server is invalid", ex);
      try {
        apiContainer.getProxyV2API().proxyError(agentId, "Configuration file is invalid: " +
            ex.toString());
      } catch (Exception e) {
        logger.log(Level.WARNING, "cannot report error to collector", e);
      }
      return null;
    }
    return newConfig;
  }

  /**
   * Actual agents can do additional configuration.
   *
   * @param config The configuration to process.
   */
  protected void processConfiguration(AgentConfiguration config) {
    try {
      apiContainer.getProxyV2API().proxyConfigProcessed(agentId);
    } catch (RuntimeException e) {
      // cannot throw or else configuration update thread would die.
    }
  }

  protected void setupCheckins() {
    // Poll or read the configuration file to use.
    AgentConfiguration config;
    if (proxyConfig.getConfigFile() != null) {
      logger.info("Loading configuration file from: " + proxyConfig.getConfigFile());
      try {
        config = JSON_PARSER.readValue(new FileReader(proxyConfig.getConfigFile()),
            AgentConfiguration.class);
      } catch (IOException e) {
        logger.log(Level.SEVERE, "cannot read from config file", e);
        throw new RuntimeException("Cannot read from config file: " + proxyConfig.getConfigFile());
      }
      try {
        config.validate(localAgent);
      } catch (RuntimeException ex) {
        logger.log(Level.SEVERE, "cannot parse config file", ex);
        throw new RuntimeException("cannot parse config file", ex);
      }
      agentId = null;
    } else {
      updateAgentMetrics.run();
      config = checkin();
      if (config == null && retryCheckin) {
        // immediately retry check-ins if we need to re-attempt
        // due to changing the server endpoint URL
        updateAgentMetrics.run();
        config = checkin();
      }
      logger.info("scheduling regular check-ins");
      agentConfigurationExecutor.scheduleAtFixedRate(updateAgentMetrics, 10, 60, TimeUnit.SECONDS);
      agentConfigurationExecutor.scheduleWithFixedDelay(updateConfiguration, 0, 1, TimeUnit.SECONDS);
    }
    // 6. Setup work units and targets based on the configuration.
    if (config != null) {
      logger.info("initial configuration is available, setting up proxy");
      processConfiguration(config);
      hadSuccessfulCheckin = true;
    }
  }

  public void shutdown() {
    if (!shuttingDown.compareAndSet(false, true)) return;
    try {
      try {
        logger.info("Shutting down the proxy...");
      } catch (Throwable t) {
        // ignore logging errors
      }

      System.out.println("Shutting down: Stopping listeners...");
      stopListeners();

      System.out.println("Shutting down: Stopping schedulers...");
      managedExecutors.forEach(ExecutorService::shutdownNow);
        // wait for up to request timeout
      managedExecutors.forEach(x -> {
        try {
          x.awaitTermination(proxyConfig.getHttpRequestTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // ignore
        }
      });

      System.out.println("Shutting down: Running finalizing tasks...");
      shutdownTasks.forEach(Runnable::run);

      System.out.println("Shutdown complete.");
    } catch (Throwable t) {
      try {
        logger.log(Level.SEVERE, "Error during shutdown: ", t);
      } catch (Throwable loggingError) {
        t.addSuppressed(loggingError);
        t.printStackTrace();
      }
    }
  }

  protected abstract void startListeners();

  protected abstract void stopListeners();

  protected abstract void configureTokenAuthenticator();

  abstract void setupMemoryGuard(double threshold);
}
