package com.wavefront.agent.logforwarder;

import com.vmware.log.forwarder.httpclient.HttpClientUtils;
import com.vmware.log.forwarder.services.AboutInfoServiceV2;
import com.vmware.log.forwarder.services.BaseService;
import com.vmware.log.forwarder.services.DisplayFileContentsService;
import com.vmware.log.forwarder.services.GenerateThreadDumpServiceV2;
import com.vmware.log.forwarder.services.LogForwarderHealthService;
import com.vmware.log.forwarder.services.LogForwarderRuntimeSummary;
import com.vmware.log.forwarder.services.MetricsSummaryServiceV2;
import com.vmware.log.forwarder.verticle.MainVerticle;
import com.vmware.log.forwarder.verticle.VertxUtils;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;
import com.wavefront.agent.logforwarder.services.LogForwarderConfigService;
import com.vmware.log.forwarder.httpclient.HttpClientUtils;
import com.vmware.log.forwarder.lemansclient.LemansClientState;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.IngestionClient;

import org.apache.commons.lang.StringUtils;

import static com.vmware.log.forwarder.host.LogForwarderConfigProperties.EVENT_FORWARDING_HTTP_CLIENT;
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

    Map<String, BaseService> baseServiceMap = new HashMap<>();

    LogForwarderHealthService logForwarderHealthService = new LogForwarderHealthService();

    HttpClientUtils.createAsyncHttpClient(EVENT_FORWARDING_HTTP_CLIENT, 30_000, Boolean.FALSE);

    if (StringUtils.isNotEmpty(LogForwarderUtils.getLemansServerUrl()) &&
        StringUtils.isNotEmpty(LogForwarderUtils.getLemansClientAccessKey())) {
      logger.info("Enabled Vert.x flow for log-forwarder agent.");
      IngestionClient.getInstance().initializeVertxLemansClient(LogForwarderUtils.getLemansServerUrl(),
          LogForwarderUtils.getLemansClientAccessKey());
    }

    DisplayFileContentsService displayFileContentsService = new DisplayFileContentsService();
    LogForwarderRuntimeSummary logForwarderRuntimeSummary = new LogForwarderRuntimeSummary();
    AboutInfoServiceV2 aboutInfoService = new AboutInfoServiceV2();
    GenerateThreadDumpServiceV2 generateThreadDumpService = new GenerateThreadDumpServiceV2();
    MetricsSummaryServiceV2 metricsSummaryService = new MetricsSummaryServiceV2();
    LogForwarderConfigService logForwarderConfigService = new LogForwarderConfigService();

    baseServiceMap.put(LogForwarderHealthService.SELF_LINK, logForwarderHealthService);
    baseServiceMap.put(DisplayFileContentsService.SELF_LINK, displayFileContentsService);
    baseServiceMap.put(LogForwarderRuntimeSummary.SELF_LINK, logForwarderRuntimeSummary);
    baseServiceMap.put(AboutInfoServiceV2.SELF_LINK, aboutInfoService);
    baseServiceMap.put(GenerateThreadDumpServiceV2.SELF_LINK, generateThreadDumpService);
    baseServiceMap.put(MetricsSummaryServiceV2.SELF_LINK, metricsSummaryService);
    baseServiceMap.put(LogForwarderConfigService.SELF_LINK, logForwarderConfigService);
    //TODO FIGURE OUT IF THIS IS NEEDED seems like a alert on general health of forwarder
//    LogForwarderConfigProperties.systemAlertsEvaluatorService.scheduleAlertEvaluator();
    //TODO MOVE THIS TO POST TO WAVEFRONT DIRECTLY as part of proxy metrics?
    LogForwarderConfigProperties.telemetryService.schedulePeriodicTaskToPostMetrics();
    //TODO Figure this out if not needed for sure
//    LogForwarderConfigProperties.generalSettingsService.scheduleGlobalConfigRefresh();

    LogForwarderConfigProperties.logForwarderVertx =
        MainVerticle.deploy(baseServiceMap, 1, VertxUtils.getLogForwarderVertxPort());

    LogForwarderUtils.autoInitConfig("processor-config.json");

//    if (LogForwarderUtils.autoInitializeEnabled()) {
//      LogForwarderConfigProperties.vSphereEventCollector.startScheduler();
//    }
    logger.info("Successfully started the log forwarder....");
  }


  public void stopListeners() {
    logger.info("Started logforwarder shutdown hook..");
    LemansClientState.accessKeyVsLemansClientHost.clear();
    LogForwarderConfigProperties.telemetryService.stopTimerTask();
//    LogForwarderConfigProperties.generalSettingsService.stopGlobalConfigRefresh();

    LemansClientState.accessKeyVsLemansClient.forEach((lemansAccessKey, lemansClient) -> {
      lemansClient.stop();
    });
    LemansClientState.accessKeyVsLemansClient.clear();

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
