package com.wavefront.agent.logforwarder.ingestion.util;

/**
 * TODO This class is more than a Util and misleading consider renaming it. This starts lifecycle
 * of REST endpoints etc
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/2/21 4:41 PM
 */
/*
 * Copyright (c) 2019 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

import static com.vmware.log.forwarder.host.LogForwarderConfigProperties.EVENT_FORWARDING_HTTP_CLIENT;

import java.io.FileInputStream;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.regions.Regions;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.Vertx;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.vmware.lemans.client.gateway.LemansClient;
import com.vmware.log.forwarder.host.LogForwarderArgs;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.vmware.log.forwarder.httpclient.HttpClientUtils;
import com.vmware.log.forwarder.lemansclient.LemansClientState;
import com.vmware.log.forwarder.lemansclient.LogForwarderAgentHost;
import com.vmware.log.forwarder.restapi.RestApiVerticle;
import com.vmware.log.forwarder.services.BaseService;
import com.vmware.log.forwarder.services.EventForwardingService;
import com.vmware.log.forwarder.verticle.VertxUtils;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.wavefront.agent.logforwarder.constants.LogForwarderConstants;
import com.wavefront.agent.logforwarder.ingestion.processors.Processor;
import com.wavefront.agent.logforwarder.ingestion.processors.config.ComponentConfig;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventBatch;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventPayload;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.FieldConstants;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.StructureFactory;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.api.StructureParser;
import com.wavefront.agent.logforwarder.ingestion.restapi.LogForwarderRestIngestEndpoint;
import com.wavefront.agent.logforwarder.services.LogForwarderConfigService;

public class LogForwarderUtils {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static String REFERER_HEADER = HttpHeaderNames.REFERER.toString();

  private static StructureFactory structureFactory = new StructureFactory();

  /**
   * concatenate the syslog message with priority
   * the priority is hardcoded as `99`
   *
   * @param syslogMsg
   * @return
   */
  public static String concatSyslogMessageWithPriority(String syslogMsg) {
    return "<99>".concat(syslogMsg);
  }

  /**
   * convert long time to string if time is not null
   *
   * @param timeLong time in long
   * @return time in string
   */
  public static String longTimeToString(Long timeLong) {
    String pattern = "yyyy-MM-dd HH:mm:ss";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    return simpleDateFormat.format(new Date(timeLong));
  }

  public static void processLogForwarderArgs(String[] args) {
    //TODO Are these LogForwarderArgs org types needed? Likely not remove them. Looks like
    // internal vmware requirement
    LogForwarderConfigProperties.logForwarderArgs = new LogForwarderArgs();
    CommandLineArgumentParser.parse(LogForwarderConfigProperties.logForwarderArgs, args);

    LogForwarderConfigProperties.logForwarderArgs.sddcId
        = removeDoubleQuotesFromBeginningAndEnd(LogForwarderConfigProperties.logForwarderArgs.sddcId);
    LogForwarderConfigProperties.logForwarderArgs.orgId
        = removeDoubleQuotesFromBeginningAndEnd(LogForwarderConfigProperties.logForwarderArgs.orgId);
    LogForwarderConfigProperties.logForwarderArgs.region
        = removeDoubleQuotesFromBeginningAndEnd(LogForwarderConfigProperties.logForwarderArgs.region);
    LogForwarderConfigProperties.logForwarderArgs.orgType
        = removeDoubleQuotesFromBeginningAndEnd(LogForwarderConfigProperties.logForwarderArgs.orgType);
    LogForwarderConfigProperties.logForwarderArgs.sddcEnv
        = removeDoubleQuotesFromBeginningAndEnd(LogForwarderConfigProperties.logForwarderArgs.sddcEnv);
    LogForwarderConfigProperties.logForwarderArgs.dimensionMspMasterOrgId
        = removeDoubleQuotesFromBeginningAndEnd(LogForwarderConfigProperties
        .logForwarderArgs.dimensionMspMasterOrgId);

    LogForwarderConfigProperties.sddcId = LogForwarderConfigProperties.logForwarderArgs.sddcId;
    LogForwarderConfigProperties.orgId = LogForwarderConfigProperties.logForwarderArgs.orgId;

    if (LogForwarderConfigProperties.logForwarderArgs.addFwderIdInEvent != null) {
      LogForwarderConfigProperties.addFwderIdInEvent
          = Boolean.parseBoolean(LogForwarderConfigProperties.logForwarderArgs.addFwderIdInEvent);
    }

    logger.info("hostArguments=" + LogForwarderConfigProperties.logForwarderArgs);
  }

  private static String removeDoubleQuotesFromBeginningAndEnd(String str) {
    if (!StringUtils.isEmpty(str)) {
      if (str.startsWith("\"")) {
        str = str.substring(1, str.length());
      }
      if (str.endsWith("\"")) {
        str = str.substring(0, str.length() - 1);
      }
    }
    return str;
  }

  public static DeferredResult<Operation> postToEventForwardingService(ServiceHost serviceHost, String body) {
    Operation operation = Operation
        .createPost(serviceHost, EventForwardingService.SELF_LINK)
        .setBody(body)
        .setReferer(serviceHost.getUri());
    return serviceHost.sendWithDeferredResult(operation)
        .exceptionally(e -> {
          logger.error("BiDirectionalPipeline -- error in call to EventForwardingService worflow", e);
          return null;
        });
  }

  public static void postToEventForwardingService(URI baseUri, String body) {
    CloseableHttpAsyncClient httpAsyncClient = HttpClientUtils.getHttpClient(EVENT_FORWARDING_HTTP_CLIENT);
    URI url = UriUtils.extendUri(baseUri, EventForwardingService.SELF_LINK);

    HttpPost httpPost = new HttpPost(url);
    httpPost.addHeader("Content-Type", "application/json");
    httpPost.addHeader("Accept", "application/json");
    httpPost.setEntity(new StringEntity(body, StandardCharsets.UTF_8));

    httpAsyncClient.execute(httpPost, new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse httpResponse) {
        int responseCode = httpResponse.getStatusLine().getStatusCode();
        if (!((responseCode >= 200) && (responseCode <= 299))) {
          logger.error("BiDirectionalPipeline -- error in call to EventForwardingService work flow, +" +
              "status code " + responseCode);
        }
      }

      @Override
      public void failed(Exception e) {
        logger.error("BiDirectionalPipeline -- error in call to EventForwardingService work flow", e);
      }

      @Override
      public void cancelled() {
        logger.error("BiDirectionalPipeline -- error in call to EventForwardingService " +
            "work flow(request cancelled)");
      }
    });
  }


  public static void disableHttp2IfNeeded() {
    if (LogForwarderUtils.getLemansServerUrl() != null &&
        !(LogForwarderUtils.getLemansServerUrl().startsWith("https"))) {
      System.getProperties().setProperty(LogForwarderConstants.DISABLE_HTTP2_PROPERTY, Boolean.TRUE.toString());
    }
  }

  public static String getLemansClientAccessKey() {
    if (LogForwarderConfigProperties.logForwarderArgs.logIqAccessKey != null) {
      return LogForwarderConfigProperties.logForwarderArgs.logIqAccessKey;
    } else if (LogForwarderConfigProperties.logForwarderArgs.lemansAccessKey != null) {
      return LogForwarderConfigProperties.logForwarderArgs.lemansAccessKey;
    }

    return LogForwarderConfigProperties.cspAuthenticationKey;
  }

  public static String getLemansServerUrl() {
    if (LogForwarderConfigProperties.logForwarderArgs.logIqUrl != null) {
      return LogForwarderConfigProperties.logForwarderArgs.logIqUrl;
    }
    return LogForwarderConfigProperties.logForwarderArgs.lemansServerUrl;
  }

  public static String getForwarderId() {
    String forwarderId = null;
    if (LogForwarderConfigProperties.sddcId != null) {
      forwarderId = LogForwarderConfigProperties.sddcId;
    } else if (LogForwarderConfigProperties.logForwarderArgs.proxyId != null) {
      forwarderId = LogForwarderConfigProperties.logForwarderArgs.proxyId;
    } else if (LogForwarderConfigProperties.logForwarderArgs.csgwId != null) {
      forwarderId = LogForwarderConfigProperties.logForwarderArgs.csgwId;
      if (LogForwarderConfigProperties.logForwarderArgs.vcId != null) {
        forwarderId = forwarderId + "-" + LogForwarderConfigProperties.logForwarderArgs.vcId;
      }
    }
    return forwarderId;
  }

  public static String getCspGazUri() {
    return LogForwarderConfigProperties.logForwarderArgs.cspGazUri;
  }

  public static String getserviceAuthId() {
    return LogForwarderConfigProperties.logForwarderArgs.cspGazAuthId;
  }

  public static String getserviceAuthSecret() {
    return LogForwarderConfigProperties.logForwarderArgs.cspGazAuthSecret;
  }

  public static void startRestApiHosts(List<ComponentConfig> componentConfigs) {
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
              logger.info("Starting Rest API Verticle for port = {}", httpPort);
              Vertx restApiVerticle = deployRestApiVerticle(component);
              LogForwarderConfigProperties.respApiVerticles.put(httpPort, restApiVerticle);
            } catch (Throwable e) {
              throw new RuntimeException(e);
            }
          }
        });

    logger.info("Rest API Verticles started successfully");
  }

  public static Vertx deployRestApiVerticle(ComponentConfig componentConfig) {
    /** start log-forwarder ingestion api that accepts simple json format */
    LogForwarderRestIngestEndpoint lfService = new LogForwarderRestIngestEndpoint(componentConfig.component);
    /** start log-insight ingestion api that accepts cfapi format */
    LogForwarderRestIngestEndpoint liService = new LogForwarderRestIngestEndpoint(componentConfig.component);
    // TODO remove the below We are not bringing in LI agents remove dependency here
//    LogInsightAgentStatusService logInsightAgentStatusService = new LogInsightAgentStatusService();
//    LogInsightAgentsTelemetryService logInsightAgentsTelemetryService = new LogInsightAgentsTelemetryService();
//    LogInsightAgentPackageInfoService logInsightAgentPackageInfoService = new LogInsightAgentPackageInfoService();
//    LogInsightAgentPackageDownloadService logInsightAgentPackageDownloadService = new
//        LogInsightAgentPackageDownloadService();

    Map<String, BaseService> serviceMap = new HashMap<>();
    serviceMap.put(LogForwarderRestIngestEndpoint.SELF_LINK, liService);
    serviceMap.put(LogForwarderRestIngestEndpoint.SELF_LINK, lfService);
//    serviceMap.put(LogInsightAgentStatusService.SELF_LINK, logInsightAgentStatusService);
//    serviceMap.put(LogInsightAgentsTelemetryService.SELF_LINK, logInsightAgentsTelemetryService);
//    serviceMap.put(LogInsightAgentPackageInfoService.SELF_LINK, logInsightAgentPackageInfoService);
//    serviceMap.put(LogInsightAgentPackageDownloadService.SELF_LINK, logInsightAgentPackageDownloadService);

    // Initialize Vert.X
    Vertx restApiVertx = RestApiVerticle.deploy(serviceMap, Runtime.getRuntime().availableProcessors(),
        VertxUtils.getVertxRestApiPort(componentConfig.httpPort));
    return restApiVertx;
  }

//  public static void startSyslogServers(List<ComponentConfig> componentConfigs) {
//    logger.info("Starting syslog servers");
//
//    componentConfigs
//        .stream()
//        .forEach((component) -> {
//          if (component.syslogPort != -1) {
//            try {
//              SyslogIngestionServer tcpServer = new SyslogIngestionServer(
//                  SyslogConstants.TCP, component);
//              SyslogIngestionServer udpServer = new SyslogIngestionServer(
//                  SyslogConstants.UDP, component);
//              tcpServer.start();
//              udpServer.start();
//            } catch (Exception e) {
//              throw new RuntimeException(e);
//            }
//          }
//        });
//
//    logger.info("syslog servers started successfully");
//  }

  @SuppressWarnings("unchecked")
  public static List<ComponentConfig> parseForwarderConfigAndCreateProcessors(String configJSON) throws Exception {
    JSONArray jsonArray = (JSONArray) new JSONParser().parse(configJSON);
    List<ComponentConfig> componentConfigs = new ArrayList<>();
    jsonArray.forEach((componentConfig) -> {
      JSONObject componentConfigJSON = (JSONObject) componentConfig;
      JSONArray processorsJSONArray = (JSONArray) ((JSONObject) componentConfig).get("processors");

      ComponentConfig componentConfigObj = new ComponentConfig();
      componentConfigObj.component = componentConfigJSON.get("component").toString();
      componentConfigObj.syslogPort = Integer.parseInt(componentConfigJSON.get("syslogPort") != null
          ? componentConfigJSON.get("syslogPort").toString()
          : "-1");
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
            Processor createdProcessor = LogForwarderUtils
                .createProcessor(processorClass, processorConfig);
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

  public static Processor createProcessor(
      Object processorClass,
      Object processorConfig) throws Throwable {
    Processor processor = (Processor) Class.forName(processorClass.toString()).newInstance();
    processor.initializeProcessor((JSONAware) processorConfig);
    logger.info("created processor " + processor);
    return processor;
  }

  /**
   * auto-initialize the vmc-config if
   * {@link com.vmware.log.forwarder.host.LogForwarderArgs#autoInitializeConfig} is true
   * <p>
   * the vmc config is loaded from vmc-config.json that is present in the classpath
   */
  public static void autoInitConfig(String configFileName) {
    String autoInitConfig = LogForwarderConfigProperties.logForwarderArgs.autoInitializeConfig;
    if (autoInitConfig != null) {
      autoInitConfig = autoInitConfig.trim().toLowerCase();
    }
    if (autoInitConfig != null && autoInitConfig.equals("true")) {
      logger.info("auto-initialize config is true, so initializing the config");
      ClassLoader classLoader = LogForwarderUtils.class.getClassLoader();
      try {
        String config = IOUtils.toString(classLoader.getResourceAsStream(configFileName),
            StandardCharsets.UTF_8);
        logger.info("config=" + config);
        LogForwarderConfigService.startSyslogAndRestApiServices(config);
      } catch (Exception e) {
        logger.error("POST auto-config failed", e);
      }
    } else {
      logger.info("auto-initialize config is not required");
    }
  }

  public static boolean autoInitializeEnabled() {
    String autoInitConfig = LogForwarderConfigProperties.logForwarderArgs.autoInitializeConfig;
    if (autoInitConfig != null) {
      autoInitConfig = autoInitConfig.trim().toLowerCase();
    }
    if (autoInitConfig != null && autoInitConfig.equals("true")) {
      return true;
    }
    return false;
  }

  public static void setBuildNumberProperty(String fileName) {
    String buildNumer = "latest";
    try {
      Properties codeProperties = new Properties();
      codeProperties.load(new FileInputStream(fileName));
      buildNumer = codeProperties.getProperty(LogForwarderConstants.BUILD_NUMBER);
      logger.info("buildNumber=" + buildNumer + ", build file name=" + fileName +
          " codeProperties=" + codeProperties);
    } catch (Exception e) {
      logger.warn("Resource file not found: " + fileName);
    }
    System.setProperty(LogForwarderConstants.BUILD_NUMBER, buildNumer);
  }

  /**
   * get the AWS region from {@link Regions#getName()}, if that fails get the AWS region from enum
   *
   * @param regionString region in string
   * @return AWS region
   */
  public static Regions getAWSRegion(String regionString) {
    if (StringUtils.isNotEmpty(regionString)) {
      try {
        try {
          return Regions.fromName(regionString);
        } catch (IllegalArgumentException e) {
          return Regions.valueOf(regionString);
        }
      } catch (Exception e) {
        logger.error("aws region is wrong region=" + regionString);
        return null;
      }
    }
    return null;
  }

  /**
   * parse the "body" field in event payload with the passed structure
   *
   * @param payload   event payload, not null
   * @param structure structure, not null
   */
  public static void parseJSON(EventPayload payload, StructureFactory.Structure structure) {
    EventBatch batch = payload.batch;
    EventBatch newBatch = new EventBatch();

    // lookup the parser
    StructureParser parser = structureFactory.getParser(structure);

    if (payload.inputReader != null) {
      try {
        Iterable<Event> events = parser.parse(payload.inputReader);
        for (Iterator<Event> iterator = events.iterator(); iterator.hasNext(); ) {
          Event event2 = iterator.next();
          newBatch.add(event2);
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    } else {
      batch.forEach(event -> {
        try {
          // parse json
          String jsonBody = (String) event.get("body");
          StringReader reader = new StringReader(jsonBody);
          Iterable<Event> events = parser.parse(reader);
          for (Iterator<Event> iterator = events.iterator(); iterator.hasNext(); ) {
            Event event2 = iterator.next();
            newBatch.add(event2);
          }
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
    }


    // add new entries
    payload.batch = newBatch;
  }

  /**
   * populate source field for all events in the event payload
   *
   * @param eventPayload event payload, not null
   */
  public static void populateSource(EventPayload eventPayload) {
    String referer = eventPayload.requestHeaders.get(REFERER_HEADER);
    if (referer != null) {
      eventPayload.batch.forEach(e -> e.putIfAbsent(FieldConstants.SOURCE, referer));
    }
    eventPayload.requestHeaders.remove(REFERER_HEADER);
  }

  /**
   * add forwarder id to the events in the event payload if flag is enabled.
   *
   * @param eventPayload event payload, not null
   */
  public static void addForwarderIdIfNeeded(EventPayload eventPayload) {
    if (LogForwarderConfigProperties.addFwderIdInEvent) {
      eventPayload.batch.forEach((event) -> {
        event.putIfAbsent(LogForwarderConstants.FORWARDER_ID, LogForwarderUtils.getForwarderId());
      });
    }
  }

  /**
   * Returns Lemans Agent Id.
   */
  public static String getLemansAgentId() {
    if (!LogForwarderConfigProperties.logForwarderArgs.enableVertx) {
      LogForwarderAgentHost agentHost = LemansClientState.accessKeyVsLemansClientHost
          .get(LogForwarderUtils.getLemansClientAccessKey());
      return agentHost != null ? agentHost.getAgentId() : null;
    } else {
      LemansClient lemansClient = LemansClientState.accessKeyVsLemansClient
          .get(LogForwarderUtils.getLemansClientAccessKey());
      return lemansClient != null ? lemansClient.getAgentId() : null;
    }
  }
}
