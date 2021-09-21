package com.wavefront.agent.logforwarder.ingestion.client.gateway.constants;

/**
 * SaaS Side ingestion gateway URIs
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/20/21 2:34 PM
 */
public class IngestionGatewayUris {
  public static final String LEMANS_PREFIX = "/le-mans";
  public static final String STREAMS = "/streams";
  public static final String ROUTERS = "/routers";
  public static final String LEMANS_V1_PREFIX = "/le-mans/v1";
  public static final String LEMANS_V2_PREFIX = "/le-mans/v2";
  public static final String ROUTERS_PREFIX = "/le-mans/v1/routers";
  public static final String SYNCHRONOUS_EXECUTOR_SERVICE = "/le-mans/synchronous-command-executor";
  public static final String MESSAGE_CONSUMER_SERVICE = "/le-mans/message-consumer";
  public static final String SESSIONS_SERVICE_V2 = "/le-mans/v2/sessions";
  public static final String MESSAGE_EGRESS_SERVICE_V2 = "/le-mans/v2/message-egress";
  public static final String AGENTS_SERVICE = "/le-mans/v1/agents";
  public static final String COMMANDS_SERVICE = "/le-mans/v1/commands";
  public static final String STREAM_INGRESS = "/le-mans/v1/streams";

  private IngestionGatewayUris() {
  }

}
