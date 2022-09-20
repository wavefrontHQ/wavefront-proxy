package com.wavefront.agent;

import static com.wavefront.agent.ProxyContext.entityPropertiesFactoryMap;
import static com.wavefront.agent.ProxyUtil.getOrCreateProxyId;
import static com.wavefront.common.Utils.*;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.config.LogsIngestionConfig;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.senders.SenderTasksManager;
import com.wavefront.agent.data.EntityPropertiesFactoryImpl;
import com.wavefront.agent.logsharvesting.InteractiveLogsTester;
import com.wavefront.agent.preprocessor.*;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.net.ssl.SSLException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;

/**
 * Agent that runs remotely on a server collecting metrics.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public abstract class AbstractAgent {
  protected static final Logger logger = Logger.getLogger("proxy");
  /** A set of commandline parameters to hide when echoing command line arguments */
  protected static final Set<String> PARAMETERS_TO_HIDE =
      ImmutableSet.of("-t", "--token", "--proxyPassword");

  protected final ProxyConfig proxyConfig = new ProxyConfig();
  protected final List<ExecutorService> managedExecutors = new ArrayList<>();
  protected final List<Runnable> shutdownTasks = new ArrayList<>();
  protected final PreprocessorConfigManager preprocessors = new PreprocessorConfigManager();
  protected final ValidationConfiguration validationConfiguration = new ValidationConfiguration();
  protected final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  protected final AtomicBoolean truncate = new AtomicBoolean(false);
  final Counter activeListeners =
      Metrics.newCounter(ExpectedAgentMetric.ACTIVE_LISTENERS.metricName);
  protected APIContainer apiContainer;
  protected ProxyCheckInScheduler proxyCheckinScheduler;
  protected UUID agentId;
  protected SslContext sslContext;
  protected List<Integer> tlsPorts = new ArrayList<>();
  protected boolean secureAllPorts = false;

  @Deprecated
  public AbstractAgent(boolean localAgent, boolean pushAgent) {
    this();
  }

  public AbstractAgent() {
    entityPropertiesFactoryMap.put(
        APIContainer.CENTRAL_TENANT_NAME, new EntityPropertiesFactoryImpl(proxyConfig));
  }

  private void addPreprocessorFilters(String ports, String allowList, String blockList) {
    if (ports != null && (allowList != null || blockList != null)) {
      csvToList(ports)
          .forEach(
              port -> {
                PreprocessorRuleMetrics ruleMetrics =
                    new PreprocessorRuleMetrics(
                        Metrics.newCounter(
                            new TaggedMetricName(
                                "validationRegex",
                                "points-rejected",
                                "port",
                                String.valueOf(port))),
                        Metrics.newCounter(
                            new TaggedMetricName(
                                "validationRegex", "cpu-nanos", "port", String.valueOf(port))),
                        Metrics.newCounter(
                            new TaggedMetricName(
                                "validationRegex",
                                "points-checked",
                                "port",
                                String.valueOf(port))));
                if (blockList != null) {
                  preprocessors
                      .getSystemPreprocessor(port)
                      .forPointLine()
                      .addFilter(new LineBasedBlockFilter(blockList, ruleMetrics));
                }
                if (allowList != null) {
                  preprocessors
                      .getSystemPreprocessor(port)
                      .forPointLine()
                      .addFilter(new LineBasedAllowFilter(allowList, ruleMetrics));
                }
              });
    }
  }

  @VisibleForTesting
  void initSslContext() throws SSLException {
    if (!isEmpty(proxyConfig.getPrivateCertPath()) && !isEmpty(proxyConfig.getPrivateKeyPath())) {
      sslContext =
          SslContextBuilder.forServer(
                  new File(proxyConfig.getPrivateCertPath()),
                  new File(proxyConfig.getPrivateKeyPath()))
              .build();
    }
    if (!isEmpty(proxyConfig.getTlsPorts()) && sslContext == null) {
      Preconditions.checkArgument(
          sslContext != null, "Missing TLS certificate/private key configuration.");
    }
    if (StringUtils.equals(proxyConfig.getTlsPorts(), "*")) {
      secureAllPorts = true;
    } else {
      tlsPorts = csvToList(proxyConfig.getTlsPorts());
    }
  }

  private void initPreprocessors() {
    String configFileName = proxyConfig.getPreprocessorConfigFile();
    if (configFileName != null) {
      try {
        preprocessors.loadFile(configFileName);
        preprocessors.setUpConfigFileMonitoring(configFileName, 5000); // check every 5s
      } catch (FileNotFoundException ex) {
        throw new RuntimeException(
            "Unable to load preprocessor rules - file does not exist: " + configFileName);
      }
      logger.info("Preprocessor configuration loaded from " + configFileName);
    }

    // convert block/allow list fields to filters for full backwards compatibility.
    // "block" and "allow" regexes are applied to pushListenerPorts, graphitePorts and picklePorts
    String allPorts =
        StringUtils.join(
            new String[] {
              ObjectUtils.firstNonNull(proxyConfig.getPushListenerPorts(), ""),
              ObjectUtils.firstNonNull(proxyConfig.getGraphitePorts(), ""),
              ObjectUtils.firstNonNull(proxyConfig.getPicklePorts(), ""),
              ObjectUtils.firstNonNull(proxyConfig.getTraceListenerPorts(), "")
            },
            ",");
    addPreprocessorFilters(allPorts, proxyConfig.getAllowRegex(), proxyConfig.getBlockRegex());
    // opentsdb block/allow lists are applied to opentsdbPorts only
    addPreprocessorFilters(
        proxyConfig.getOpentsdbPorts(),
        proxyConfig.getOpentsdbAllowRegex(),
        proxyConfig.getOpentsdbBlockRegex());
  }

  // Returns null on any exception, and logs the exception.
  protected LogsIngestionConfig loadLogsIngestionConfig() {
    try {
      if (proxyConfig.getLogsIngestionConfigFile() == null) {
        return null;
      }
      ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
      return objectMapper.readValue(
          new File(proxyConfig.getLogsIngestionConfigFile()), LogsIngestionConfig.class);
    } catch (UnrecognizedPropertyException e) {
      logger.severe("Unable to load logs ingestion config: " + e.getMessage());
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Could not load logs ingestion config", e);
    }
    return null;
  }

  private void postProcessConfig() {
    // disable useless info messages when httpClient has to retry a request due to a stale
    // connection. the alternative is to always validate connections before reuse, but since
    // it happens fairly infrequently, and connection re-validation performance penalty is
    // incurred every time, suppressing that message seems to be a more reasonable approach.
    // org.apache.log4j.Logger.getLogger("org.apache.http.impl.execchain.RetryExec").
    //     setLevel(org.apache.log4j.Level.WARN);
    // Logger.getLogger("org.apache.http.impl.execchain.RetryExec").setLevel(Level.WARNING);

    if (StringUtils.isBlank(proxyConfig.getHostname().trim())) {
      throw new IllegalArgumentException(
          "hostname cannot be blank! Please correct your configuration settings.");
    }
  }

  @VisibleForTesting
  void parseArguments(String[] args) {
    // read build information and print version.
    String versionStr =
        "Wavefront Proxy version "
            + getBuildVersion()
            + " (pkg:"
            + getPackage()
            + ")"
            + ", runtime: "
            + getJavaVersion();
    try {
      if (!proxyConfig.parseArguments(args, this.getClass().getCanonicalName())) {
        System.exit(0);
      }
    } catch (ParameterException e) {
      logger.info(versionStr);
      logger.severe("Parameter exception: " + e.getMessage());
      System.exit(1);
    }
    logger.info(versionStr);
    logger.info(
        "Arguments: "
            + IntStream.range(0, args.length)
                .mapToObj(
                    i -> (i > 0 && PARAMETERS_TO_HIDE.contains(args[i - 1])) ? "<HIDDEN>" : args[i])
                .collect(Collectors.joining(" ")));
    proxyConfig.verifyAndInit();
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
      initSslContext();
      initPreprocessors();

      if (proxyConfig.isTestLogs()
          || proxyConfig.getTestPreprocessorForPort() != null
          || proxyConfig.getTestSpanPreprocessorForPort() != null) {
        InteractiveTester interactiveTester;
        if (proxyConfig.isTestLogs()) {
          logger.info("Reading line-by-line sample log messages from STDIN");
          interactiveTester =
              new InteractiveLogsTester(this::loadLogsIngestionConfig, proxyConfig.getPrefix());
        } else if (proxyConfig.getTestPreprocessorForPort() != null) {
          logger.info("Reading line-by-line points from STDIN");
          interactiveTester =
              new InteractivePreprocessorTester(
                  preprocessors.get(Integer.parseInt(proxyConfig.getTestPreprocessorForPort())),
                  ReportableEntityType.POINT,
                  Integer.parseInt(proxyConfig.getTestPreprocessorForPort()),
                  proxyConfig.getCustomSourceTags());
        } else if (proxyConfig.getTestSpanPreprocessorForPort() != null) {
          logger.info("Reading line-by-line spans from STDIN");
          interactiveTester =
              new InteractivePreprocessorTester(
                  preprocessors.get(Integer.parseInt(proxyConfig.getTestPreprocessorForPort())),
                  ReportableEntityType.TRACE,
                  Integer.parseInt(proxyConfig.getTestPreprocessorForPort()),
                  proxyConfig.getCustomSourceTags());
        } else {
          throw new IllegalStateException();
        }
        //noinspection StatementWithEmptyBody
        while (interactiveTester.interactiveTest()) {
          // empty
        }
        System.exit(0);
      }

      // If we are exporting data from the queue, run export and exit
      // TODO: queue exporter
      if (proxyConfig.getExportQueueOutputFile() != null
          && proxyConfig.getExportQueuePorts() != null) {
        System.exit(0);
      }

      // 2. Read or create the unique Id for the daemon running on this machine.
      agentId = getOrCreateProxyId(proxyConfig);
      apiContainer = new APIContainer(proxyConfig, proxyConfig.isUseNoopSender());
      // config the entityPropertiesFactoryMap
      for (String tenantName : proxyConfig.getMulticastingTenantList().keySet()) {
        entityPropertiesFactoryMap.put(tenantName, new EntityPropertiesFactoryImpl(proxyConfig));
      }
      // Perform initial proxy check-in and schedule regular check-ins (once a minute)
      proxyCheckinScheduler =
          new ProxyCheckInScheduler(
              agentId,
              proxyConfig,
              apiContainer,
              this::processConfiguration,
              () -> System.exit(1),
              BuffersManager::truncateBacklog);
      proxyCheckinScheduler.scheduleCheckins();

      // Start the listening endpoints
      startListeners();

      Timer startupTimer = new Timer("Timer-startup");
      shutdownTasks.add(startupTimer::cancel);
      startupTimer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              // exit if no active listeners
              if (activeListeners.count() == 0) {
                logger.severe(
                    "**** All listener threads failed to start - there is already a "
                        + "running instance listening on configured ports, or no listening ports "
                        + "configured!");
                logger.severe("Aborting start-up");
                System.exit(1);
              }

              Runtime.getRuntime()
                  .addShutdownHook(
                      new Thread("proxy-shutdown-hook") {
                        @Override
                        public void run() {
                          shutdown();
                        }
                      });

              logger.info("setup complete");
            }
          },
          5000);
    } catch (Exception e) {
      logger.log(Level.SEVERE, e.getMessage(), e);
      //      logger.severe(e.getMessage());
      System.exit(1);
    }
  }

  /**
   * Actual agents can do additional configuration.
   *
   * @param tenantName The tenant name
   * @param config The configuration to process.
   */
  protected void processConfiguration(String tenantName, AgentConfiguration config) {
    try {
      // for all ProxyV2API
      for (String tn : proxyConfig.getMulticastingTenantList().keySet()) {
        apiContainer.getProxyV2APIForTenant(tn).proxyConfigProcessed(agentId);
      }
    } catch (RuntimeException e) {
      // cannot throw or else configuration update thread would die.
    }
  }

  /** Best-effort graceful shutdown. */
  public void shutdown() {
    if (!shuttingDown.compareAndSet(false, true)) return;
    try {
      System.out.println("Shutting down the proxy...");

      System.out.println("Shutting down: Stopping listeners...");
      stopListeners();

      System.out.println("Shutting down: Stopping Senders...");
      SenderTasksManager.shutdown();

      System.out.println("Shutting down: queues...");
      BuffersManager.shutdown();

      System.out.println("Shutting down: Stopping schedulers...");
      if (proxyCheckinScheduler != null) proxyCheckinScheduler.shutdown();

      managedExecutors.forEach(ExecutorService::shutdownNow);
      // wait for up to request timeout
      managedExecutors.forEach(
          x -> {
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

  /** Starts all listeners as configured. */
  protected abstract void startListeners() throws Exception;

  /** Stops all listeners before terminating the process. */
  protected abstract void stopListeners();
}
