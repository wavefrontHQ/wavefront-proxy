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
import com.wavefront.agent.logforwarder.ingestion.restapi.LogForwarderRestIngestEndpoint;
import com.wavefront.agent.logforwarder.ingestion.restapi.RestApiVerticle;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;
import org.apache.commons.io.IOUtils;
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
import io.vertx.core.Vertx;

/**
 * Log forwarder service does all the setup and managing of services related to log forwarding
 * functionality from proxy to wavefront cloud backend.
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/8/21 1:33 PM
 */
public class LogForwarderService {
  private static final Logger logger = Logger.getLogger("logForwarder");
  private static final String PROCESSOR_CONFIG_FILE = "processor-config.json";
  private ProxyConfig proxyConfig;

  /**
   * Constructor
   * @param proxyConfig
   */
  public LogForwarderService(ProxyConfig proxyConfig) {
    this.proxyConfig = proxyConfig;
  }

  /**
   * Starts all the services and inbound http listeners for log forwarding functionality
   * @param agentId this proxy instance unique ID
   * @throws Throwable
   */
  public void startListeners(UUID agentId) throws Throwable{
    logger.info("log-forwarder starting args=" + LogForwarderConfigProperties.logForwarderArgs);

    LogForwarderConfigProperties.logForwarderArgs.proxyId = agentId.toString();
    Map<String, BaseHttpEndpoint> BaseHttpEndpointMap = new HashMap<>();
    HttpClientUtils.createAsyncHttpClient(LogForwarderConfigProperties.EVENT_FORWARDING_HTTP_CLIENT, 30_000, Boolean.FALSE);

    logger.info("Enabled Vert.x flow for log-forwarder agent.");
    GatewayClientFactory.getInstance().initializeVertxGatewayClient(proxyConfig.getLogIngestionServerUrl(),
        proxyConfig.getLemansAccessToken(), proxyConfig.getLogForwarderDiskQueueFileLocation());

    //TODO MOVE THIS TO POST TO WAVEFRONT DIRECTLY as part of proxy metrics?
//    LogForwarderConfigProperties.telemetryService.schedulePeriodicTaskToPostMetrics();

    LogForwarderConfigProperties.logForwarderVertx =
        MainVerticle.deploy(BaseHttpEndpointMap, 1, VertxUtils.getLogForwarderVertxPort());
    // Parse xml and start listener and processor
    startProcessorsFromConfig(PROCESSOR_CONFIG_FILE);
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
    startRestApiHosts(componentConfigs);
    LogForwarderConfigProperties.logForwarderConfig = configJSON;
    logger.info("config parsed and listeners created successfully");
  }

  /**
   * Start Rest protocol Vertx vehicles to listen on config provided ports.
   * @param componentConfigs
   */
  private  void startRestApiHosts(List<ComponentConfig> componentConfigs) {
    logger.info("Starting Rest API hosts " + componentConfigs);

    Map<Integer, Vertx> respApiVerticles = LogForwarderConfigProperties.respApiVerticles;
    componentConfigs
        .stream()
        .forEach((component) -> {
          int httpPort = component.httpPort;
          if (httpPort != -1) {
            try {
              if (respApiVerticles.containsKey(httpPort)) {
                Vertx verticle = respApiVerticles.get(httpPort);
                verticle.deploymentIDs().forEach(verticle::undeploy);
                LogForwarderConfigProperties.respApiVerticles.remove(httpPort);
              }
              logger.info("Starting Rest API Verticle for port = "+ httpPort);
              Vertx restApiVerticle = deployRestApiVerticle(component);
              LogForwarderConfigProperties.respApiVerticles.put(httpPort, restApiVerticle);
            } catch (Throwable e) {
              throw new RuntimeException(e);
            }
          }
        });

    logger.info("Rest API Verticles started successfully");
  }

  /**
   * Deploy the REST API verticle with appropriate http paths
   * @param componentConfig
   * @return
   */
  private Vertx deployRestApiVerticle(ComponentConfig componentConfig) {
    /** start log-forwarder ingestion api that accepts simple json format */
    LogForwarderRestIngestEndpoint liService = new LogForwarderRestIngestEndpoint(componentConfig.component);
    // TODO remove the below We are not bringing in LI agents remove dependency here
    Map<String, BaseHttpEndpoint> serviceMap = new HashMap<>();
    serviceMap.put(LogForwarderRestIngestEndpoint.SELF_LINK, liService);

    // Initialize Vert.X
    Vertx restApiVertx = RestApiVerticle.deploy(serviceMap, Runtime.getRuntime().availableProcessors(),
        VertxUtils.getVertxRestApiPort(componentConfig.httpPort));
    return restApiVertx;
  }

  private List<ComponentConfig> parseForwarderConfigAndCreateProcessors(String configJSON) throws Exception {
    JSONArray jsonArray = (JSONArray) new JSONParser().parse(configJSON);
    List<ComponentConfig> componentConfigs = new ArrayList<>();
    jsonArray.forEach((componentConfig) -> {
      JSONObject componentConfigJSON = (JSONObject) componentConfig;
      JSONArray processorsJSONArray = (JSONArray) ((JSONObject) componentConfig).get("processors");

      ComponentConfig componentConfigObj = new ComponentConfig();
      componentConfigObj.component = componentConfigJSON.get("component").toString();
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
