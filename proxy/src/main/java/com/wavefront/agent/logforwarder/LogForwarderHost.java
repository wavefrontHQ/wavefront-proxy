package com.wavefront.agent.logforwarder;


import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.GatewayClientFactory;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.GatewayClientState;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.verticle.MainVerticle;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.verticle.VertxUtils;
import com.wavefront.agent.logforwarder.ingestion.http.client.utils.HttpClientUtils;
import com.wavefront.agent.logforwarder.ingestion.restapi.BaseHttpEndpoint;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;
import com.wavefront.agent.logforwarder.services.LogForwarderConfigService;
//import com.wavefront.agent.logforwarder.services.LogForwarderHealthService;
import org.apache.commons.lang.StringUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/8/21 1:33 PM
 */
public class LogForwarderHost {
  static final Logger logger = Logger.getLogger("logForwarder");


  public void startListeners() throws Throwable{
    logger.warning("log-forwarder starting args=" + LogForwarderConfigProperties.logForwarderArgs);

    LogForwarderUtils.setBuildNumberProperty("/opt/vmware/log-forwarder/ops-log-forwarder-build-number.properties");

    if (org.apache.commons.lang.StringUtils.isBlank(LogForwarderConfigProperties.logForwarderArgs.proxyId)) {
      if (org.apache.commons.lang.StringUtils.isBlank(LogForwarderConfigProperties.logForwarderArgs.hostId)) {
        logger.severe("HOST_ID/PROXY_ID is not present in arguments. Hence generating a random proxyId");
        LogForwarderConfigProperties.logForwarderArgs.proxyId = "random-" + UUID.randomUUID().toString();
      } else {
        LogForwarderConfigProperties.logForwarderArgs.proxyId =
            LogForwarderConfigProperties.logForwarderArgs.hostId;
      }
    }

    Map<String, BaseHttpEndpoint> BaseHttpEndpointMap = new HashMap<>();

//    LogForwarderHealthService logForwarderHealthService = new LogForwarderHealthService();

    HttpClientUtils.createAsyncHttpClient(LogForwarderConfigProperties.EVENT_FORWARDING_HTTP_CLIENT, 30_000, Boolean.FALSE);

    if (StringUtils.isNotEmpty(LogForwarderUtils.getLemansServerUrl()) &&
        StringUtils.isNotEmpty(LogForwarderUtils.getLemansClientAccessKey())) {
      logger.info("Enabled Vert.x flow for log-forwarder agent.");
      GatewayClientFactory.getInstance().initializeVertxLemansClient(LogForwarderUtils.getLemansServerUrl(),
          LogForwarderUtils.getLemansClientAccessKey());
    }
    //TODO Decide wheether following services are needed
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

    LogForwarderUtils.autoInitConfig("processor-config.json");

//    if (LogForwarderUtils.autoInitializeEnabled()) {
//      LogForwarderConfigProperties.vSphereEventCollector.startScheduler();
//    }
    logger.info("Successfully started the log forwarder....");
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
