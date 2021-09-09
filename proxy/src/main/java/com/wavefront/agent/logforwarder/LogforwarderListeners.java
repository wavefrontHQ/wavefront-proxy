package com.wavefront.agent.logforwarder;

import com.vmware.log.forwarder.processors.ComponentConfig;
import com.vmware.log.forwarder.restapi.LogForwarderIngestService;
import com.vmware.log.forwarder.restapi.LogInsightAgentPackageDownloadService;
import com.vmware.log.forwarder.restapi.LogInsightAgentPackageInfoService;
import com.vmware.log.forwarder.restapi.LogInsightAgentStatusService;
import com.vmware.log.forwarder.restapi.LogInsightAgentsTelemetryService;
import com.vmware.log.forwarder.restapi.LogInsightIngestionService;
import com.vmware.log.forwarder.restapi.RestApiVerticle;
import com.vmware.log.forwarder.services.BaseService;
import com.vmware.log.forwarder.verticle.VertxUtils;

import java.util.HashMap;
import java.util.Map;

import io.vertx.core.Vertx;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 8/25/21 4:58 PM
 */
public class LogforwarderListeners {



  public static Vertx deployRestApiVerticle(ComponentConfig componentConfig) {
    /** start log-forwarder ingestion api that accepts simple json format */
    LogForwarderIngestService lfService = new LogForwarderIngestService(componentConfig.component);
    /** start log-insight ingestion api that accepts cfapi format */
    LogInsightIngestionService liService = new LogInsightIngestionService(componentConfig.component);

    LogInsightAgentStatusService logInsightAgentStatusService = new LogInsightAgentStatusService();
    LogInsightAgentsTelemetryService logInsightAgentsTelemetryService = new LogInsightAgentsTelemetryService();
    LogInsightAgentPackageInfoService logInsightAgentPackageInfoService = new LogInsightAgentPackageInfoService();
    LogInsightAgentPackageDownloadService logInsightAgentPackageDownloadService = new
        LogInsightAgentPackageDownloadService();

    Map<String, BaseService> serviceMap = new HashMap<>();
    serviceMap.put(LogInsightIngestionService.SELF_LINK, liService);
    serviceMap.put(LogForwarderIngestService.SELF_LINK, lfService);
    serviceMap.put(LogInsightAgentStatusService.SELF_LINK, logInsightAgentStatusService);
    serviceMap.put(LogInsightAgentsTelemetryService.SELF_LINK, logInsightAgentsTelemetryService);
    serviceMap.put(LogInsightAgentPackageInfoService.SELF_LINK, logInsightAgentPackageInfoService);
    serviceMap.put(LogInsightAgentPackageDownloadService.SELF_LINK, logInsightAgentPackageDownloadService);

    // Initialize Vert.X
    Vertx restApiVertx = RestApiVerticle.deploy(serviceMap, Runtime.getRuntime().availableProcessors(),
        VertxUtils.getVertxRestApiPort(componentConfig.httpPort));
    return restApiVertx;
  }
}
