package com.wavefront.agent.logforwarder.config;

/**
 * Config properties for log forwarder
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/2/21 4:22 PM
 */
import java.util.HashMap;
import java.util.Map;

import io.vertx.core.Vertx;
import org.apache.commons.lang3.tuple.Pair;

//import com.vmware.log.forwarder.generalsettings.GeneralSettingsService;
//import com.vmware.log.forwarder.telemetry.LogForwarderTelemetryServiceV2;//TODO Move this metrics
// push to proxy metrics
import com.wavefront.agent.logforwarder.constants.LogForwarderConstants;
import com.wavefront.agent.logforwarder.ingestion.processors.config.ComponentConfig;

public class LogForwarderConfigProperties {

  public static String sddcId;
  public static String orgId;

  public static Integer minMsgsInBlob = 750;
  public static Integer minSyslogMsgsInBlob = 750;
  public static Integer inMemoryBufferSize = 200;
  public static Integer syslogMsgsBufferSize = 10000;
  public static Integer inflightOperationsCount = 200;
  public static String EVENT_FORWARDING_HTTP_CLIENT = "eventForwardingHttpClient";

  public static LogForwarderArgs logForwarderArgs;
  //TODO Move the below services out since this is about properties
//  public static final SystemAlertsEvaluatorService systemAlertsEvaluatorService = new SystemAlertsEvaluatorService();
//  public static final LogForwarderTelemetryServiceV2 telemetryService = new LogForwarderTelemetryServiceV2();
//   // TODO Remove this once finalized settings sync from cloud  will not be needed for MVP.
//  public static final GeneralSettingsService generalSettingsService = new GeneralSettingsService();

  public static Boolean addFwderIdInEvent = false;

  public static String logForwarderConfig;

  public static LogForwarderConstants.ForwarderType forwarderType;

  public static Map<String, ComponentConfig> componentConfigMap = new HashMap<>();

  /**
   * key = log forwarding config id
   * value = pair,  left= config name, right=end point utl
   */
  public static final Map<String, Pair<String, String>> logForwardingIdVsConfig = new HashMap<>();

  public static Vertx logForwarderVertx;

  public static Vertx logForwarderAgentVertx;

  public static Map<Integer, Vertx> respApiVerticles = new HashMap<>();

  public static String cspAuthenticationKey = "cspAuthenticationKey";

}
