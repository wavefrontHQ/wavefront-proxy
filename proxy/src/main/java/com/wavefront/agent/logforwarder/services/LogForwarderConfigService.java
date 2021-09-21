package com.wavefront.agent.logforwarder.services;


import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import com.vmware.xenon.common.Operation;;//TODO Remove xenon dependencies
import com.vmware.xenon.common.StatelessService;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.wavefront.agent.logforwarder.constants.LogForwarderUris;
import com.wavefront.agent.logforwarder.ingestion.processors.config.ComponentConfig;
import com.wavefront.agent.logforwarder.ingestion.restapi.BaseHttpEndpoint;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;
import com.wavefront.agent.logforwarder.ingestion.util.RequestUtil;


/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/3/21 2:36 PM
 */
public class LogForwarderConfigService extends StatelessService implements BaseHttpEndpoint {

  public static final String SELF_LINK = LogForwarderUris.LOG_FORWARDER_CONFIG_URI;

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * 1. parse the config JSON
   * 2. start the syslog listeners as needed
   * 3. start the http listeners as needed
   *
   * @param post post operation object
   */
  @Override
  public void handlePost(Operation post) {
    processPost(null, null, post);
  }

  @Override
  public void handlePost(CompletableFuture future, RoutingContext routingContext) {
    processPost(future, routingContext, null);
  }

  private void processPost(CompletableFuture future, RoutingContext routingContext, Operation post) {
    try {
      String configJSON = post != null ? post.getBody(String.class) : routingContext.getBodyAsString();
      startSyslogAndRestApiServices(configJSON);
      RequestUtil.setResponse(future, post);
    } catch (Exception e) {
      logger.error("Exception during POST config, failing the operation", e);
      RequestUtil.setException(future, post, e);
    }
  }

  public static void startSyslogAndRestApiServices(String configJSON) throws Exception {
    logger.info("Processing component config and created required listeners config: " + configJSON);
    List<ComponentConfig> componentConfigs = LogForwarderUtils.parseForwarderConfigAndCreateProcessors(configJSON);
    //TODO Remove commeented code since we are not going to add syslog support at least for now
//    LogForwarderUtils.startSyslogServers(componentConfigs);
    LogForwarderUtils.startRestApiHosts(componentConfigs);
    LogForwarderConfigProperties.logForwarderConfig = configJSON;
    logger.info("config parsed and listeners created successfully");
  }

  /**
   * return the log-forwarder config json
   *
   * @param get get operation object
   */
  @Override
  public void handleGet(Operation get) {
    get.setBody(LogForwarderConfigProperties.logForwarderConfig).complete();
  }
}