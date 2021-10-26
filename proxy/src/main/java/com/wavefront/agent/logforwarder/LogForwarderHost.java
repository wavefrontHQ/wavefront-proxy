package com.wavefront.agent.logforwarder;


import com.google.common.annotations.VisibleForTesting;

import com.wavefront.agent.ProxyConfig;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.wavefront.agent.logforwarder.constants.LogForwarderConstants;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.GatewayClientFactory;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.GatewayClientState;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.verticle.MainVerticle;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.verticle.VertxUtils;
import com.wavefront.agent.logforwarder.ingestion.http.client.utils.HttpClientUtils;
import com.wavefront.agent.logforwarder.ingestion.processors.Processor;
import com.wavefront.agent.logforwarder.ingestion.processors.config.ComponentConfig;
import com.wavefront.agent.logforwarder.ingestion.restapi.BaseHttpEndpoint;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;
import com.wavefront.agent.logforwarder.services.LogForwarderConfigService;
//import com.wavefront.agent.logforwarder.services.LogForwarderHealthService;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Log forwarder host does all the setup and managing of services related to log forwarding
 * functionality from proxy to wavefront cloud backend.
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/8/21 1:33 PM
 */
public class LogForwarderHost {
  private static final Logger logger = Logger.getLogger("logForwarder");
  private static final String PROCESSOR_CONFIG_FILE = "processor-config.json";
  private ProxyConfig proxyConfig;

  //TEST constructor
  public LogForwarderHost(ProxyConfig proxyConfig) {
    this.proxyConfig = proxyConfig;
  }

  /**
   * Starts all the services and inbound http listeners for log forwarding functionality
   * @param agentId this proxy instance unique ID
   * @throws Throwable
   */

  public void startListeners(UUID agentId) throws Throwable{
    logger.info("log-forwarder starting args=" + LogForwarderConfigProperties.logForwarderArgs);
    //    TODO remove after checking with Guru likely not needed
    //    LogForwarderUtils.setBuildNumberProperty
    //    ("/opt/vmware/log-forwarder/ops-log-forwarder-build-number.properties");

//    if (org.apache.commons.lang.StringUtils.isBlank(proxyConfig.getPro)) {
//      if (org.apache.commons.lang.StringUtils.isBlank(LogForwarderConfigProperties.logForwarderArgs.hostId)) {
//        logger.severe("HOST_ID/PROXY_ID is not present in arguments. Hence generating a random proxyId");
//        LogForwarderConfigProperties.logForwarderArgs.proxyId = "random-" + UUID.randomUUID().toString();
//      } else {
//        LogForwarderConfigProperties.logForwarderArgs.proxyId =
//            LogForwarderConfigProperties.logForwarderArgs.hostId;
//      }
//    }
    LogForwarderConfigProperties.logForwarderArgs.proxyId = agentId.toString();

    Map<String, BaseHttpEndpoint> BaseHttpEndpointMap = new HashMap<>();

//    LogForwarderHealthService logForwarderHealthService = new LogForwarderHealthService();

    HttpClientUtils.createAsyncHttpClient(LogForwarderConfigProperties.EVENT_FORWARDING_HTTP_CLIENT, 30_000, Boolean.FALSE);

    if (StringUtils.isNotEmpty(LogForwarderUtils.getLemansServerUrl()) &&
        StringUtils.isNotEmpty(LogForwarderUtils.getLemansClientAccessKey())) {
      logger.info("Enabled Vert.x flow for log-forwarder agent.");
//      GatewayClientFactory.getInstance().initializeVertxLemansClient(LogForwarderUtils.getLemansServerUrl(),
//          LogForwarderUtils.getLemansClientAccessKey());
      GatewayClientFactory.getInstance().initializeVertxLemansClient(proxyConfig.getLogIngestionServerUrl(),
          proxyConfig.getLemansAccessToken(), proxyConfig.getLogForwarderDiskQueueFileLocation());
    }
    //TODO Decide whether following services are needed
//    DisplayFileContentsService displayFileContentsService = new DisplayFileContentsService();
//    LogForwarderRuntimeSummary logForwarderRuntimeSummary = new LogForwarderRuntimeSummary();
//    AboutInfoServiceV2 aboutInfoService = new AboutInfoServiceV2();
//    GenerateThreadDumpServiceV2 generateThreadDumpService = new GenerateThreadDumpServiceV2();
//    MetricsSummaryServiceV2 metricsSummaryService = new MetricsSummaryServiceV2();
    LogForwarderConfigService logForwarderConfigService = new LogForwarderConfigService();

//    BaseHttpEndpointMap.put(LogForwarderHealthService.SELF_LINK, logForwarderHealthService);
//    BaseHttpEndpointMap.put(DisplayFileContentsService.SELF_LINK, displayFileContentsService);
//    BaseHttpEndpointMap.put(LogForwarderRuntimeSummary.SELF_LINK, logForwarderRuntimeSummary);
//    BaseHttpEndpointMap.put(AboutInfoServiceV2.SELF_LINK, aboutInfoService);
//    BaseHttpEndpointMap.put(GenerateThreadDumpServiceV2.SELF_LINK, generateThreadDumpService);
//    BaseHttpEndpointMap.put(MetricsSummaryServiceV2.SELF_LINK, metricsSummaryService);
//    BaseHttpEndpointMap.put(LogForwarderConfigService.SELF_LINK, logForwarderConfigService);
    //TODO FIGURE OUT IF THIS IS NEEDED seems like a alert on general health of forwarder
//    LogForwarderConfigProperties.systemAlertsEvaluatorService.scheduleAlertEvaluator();
    //TODO MOVE THIS TO POST TO WAVEFRONT DIRECTLY as part of proxy metrics?
//    LogForwarderConfigProperties.telemetryService.schedulePeriodicTaskToPostMetrics();
    //TODO Figure this out if not needed for sure
//    LogForwarderConfigProperties.generalSettingsService.scheduleGlobalConfigRefresh();

    LogForwarderConfigProperties.logForwarderVertx =
        MainVerticle.deploy(BaseHttpEndpointMap, 1, VertxUtils.getLogForwarderVertxPort());
    // Parse xml and start listener and processor
    startProcessorsFromConfig(PROCESSOR_CONFIG_FILE);

//    if (LogForwarderUtils.autoInitializeEnabled()) {
//      LogForwarderConfigProperties.vSphereEventCollector.startScheduler();
//    }
    logger.info("Successfully started the log forwarder....");
  }


  /**
   * Initialize processors and host http listener in configuration
   * <p>
   */
  public void startProcessorsFromConfig(String configFileName) {
      logger.info("Initializing log processors from config..");
      ClassLoader classLoader = LogForwarderUtils.class.getClassLoader();
      try {
        String config = IOUtils.toString(classLoader.getResourceAsStream(configFileName),
            StandardCharsets.UTF_8);
        logger.info("config=" + config);
        startRestApiServices(config);
      } catch (Exception e) {
        logger.log(Level.SEVERE,"POST auto-config failed", e);
      }
  }

  /**
   * Start all REST API listeners for logs
   * @param configJSON
   * @throws Exception
   */
  @VisibleForTesting
  public void startRestApiServices(String configJSON) throws Exception {
    logger.info("Processing component config and created required listeners config: " + configJSON);
    List<ComponentConfig> componentConfigs = parseForwarderConfigAndCreateProcessors(configJSON);
    LogForwarderUtils.startRestApiHosts(componentConfigs);
    LogForwarderConfigProperties.logForwarderConfig = configJSON;
    logger.info("config parsed and listeners created successfully");
  }

  private List<ComponentConfig> parseForwarderConfigAndCreateProcessors(String configJSON) throws Exception {
    JSONArray jsonArray = (JSONArray) new JSONParser().parse(configJSON);
    List<ComponentConfig> componentConfigs = new ArrayList<>();
    jsonArray.forEach((componentConfig) -> {
      JSONObject componentConfigJSON = (JSONObject) componentConfig;
      JSONArray processorsJSONArray = (JSONArray) ((JSONObject) componentConfig).get("processors");

      ComponentConfig componentConfigObj = new ComponentConfig();
      componentConfigObj.component = componentConfigJSON.get("component").toString();
//      componentConfigObj.syslogPort = Integer.parseInt(componentConfigJSON.get("syslogPort") != null
//          ? componentConfigJSON.get("syslogPort").toString()
//          : "-1");
      componentConfigObj.httpPort = Integer.parseInt(componentConfigJSON.get("httpPort") != null
          ? componentConfigJSON.get("httpPort").toString()
          : "-1");
      componentConfigObj.bufferSize = Integer.parseInt(componentConfigJSON.get("bufferSize") != null
          ? componentConfigJSON.get("bufferSize").toString()
          : "-1");
      componentConfigObj.processors = new ArrayList<>();
      componentConfigs.add(componentConfigObj);
      LogForwarderConfigProperties.componentConfigMap.put(componentConfigObj.component, componentConfigObj);

      processorsJSONArray.forEach((processor) -> {
        JSONObject processorJSON = (JSONObject) processor;
        processorJSON.forEach((processorName, processorConfig) -> {
          JSONObject processorConfigJSON = (JSONObject) processorConfig;
          String processorClass = processorConfigJSON.get("processor").toString();
          try {
            ((JSONObject) processorConfig).put(LogForwarderConstants.INGESTION_GATEWAY_ACCESS_TOKEN,
                proxyConfig.getLemansAccessToken());
            ((JSONObject) processorConfig).put(LogForwarderConstants.INGESTION_GATEWAY_URL,
                proxyConfig.getLogIngestionServerUrl());
            ((JSONObject) processorConfig).put(LogForwarderConstants.INGESTION_DISK_QUEUE_LOCATION,
                proxyConfig.getLogForwarderDiskQueueFileLocation());
            Processor createdProcessor = createProcessor(processorClass, processorConfig);
            logger.info("created processor " + createdProcessor);
            componentConfigObj.processors.add(createdProcessor);
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        });
      });
    });
    return componentConfigs;
  }

  /**
   * Create processor
   * @param processorClass
   * @param processorConfig
   * @return
   * @throws Throwable
   */
  private Processor createProcessor(
      Object processorClass,
      Object processorConfig) throws Throwable {
    Processor processor = (Processor) Class.forName(processorClass.toString()).newInstance();
    processor.initializeProcessor((JSONAware) processorConfig);
    logger.info("created processor " + processor);
    return processor;
  }



  public void stopListeners() {
    logger.info("Started logforwarder shutdown hook..");
    GatewayClientState.accessKeyVsLemansClient.clear();
//    LogForwarderConfigProperties.telemetryService.stopTimerTask();
//    LogForwarderConfigProperties.generalSettingsService.stopGlobalConfigRefresh();

    GatewayClientState.accessKeyVsLemansClient.forEach((lemansAccessKey, lemansClient) -> {
      lemansClient.stop();
    });
    GatewayClientState.accessKeyVsLemansClient.clear();

    if (LogForwarderConfigProperties.logForwarderVertx != null) {
      logger.info("Stopping log-forwarder vert.x");
      LogForwarderConfigProperties.logForwarderVertx.deploymentIDs().forEach(
          LogForwarderConfigProperties.logForwarderVertx::undeploy);
    }

    if (LogForwarderConfigProperties.logForwarderAgentVertx != null) {
      logger.info("Stopping log-forwarder agent vert.x");
      LogForwarderConfigProperties.logForwarderAgentVertx.deploymentIDs().forEach(
          LogForwarderConfigProperties.logForwarderAgentVertx::undeploy);
    }

    if (!LogForwarderConfigProperties.respApiVerticles.isEmpty()) {
      logger.info("Stopping rest API vert.x");
      LogForwarderConfigProperties.respApiVerticles.values().stream()
          .forEach(v -> v.deploymentIDs().forEach(v::undeploy));
    }
    logger.info("Finished logforwarder shutdown hook");
  }
}
