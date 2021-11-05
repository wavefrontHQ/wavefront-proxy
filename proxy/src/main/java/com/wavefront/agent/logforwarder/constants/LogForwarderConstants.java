package com.wavefront.agent.logforwarder.constants;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 8/30/21 4:50 PM
 */
public interface LogForwarderConstants {

    String TEXT_FIELD_NAME = "text";
    String TIMESTAMP_FIELD_NAME = "timestamp";
    String SOURCE_FIELD_NAME = "source";
    String SUCCESS_JSON_MSG = "{\"result\":\"SUCCESS\"}";
    String FORWARDER_ID = "forwarder_id";
    String URL = "url";
    String ENDPOINT_IDENTIFIER = "endpointIdentifier";
    String CHAIN_NAME = "chainName";
    String STREAM_NAME = "streamName";
    String BILLABLE = "billable";
    String ACCESS_KEY = "accessKey";
    String RE_INGEST_ENABLED = "reIngestEnabled";
    String ENDPOINT_TYPE = "endPointType";
    String TIMEOUT_IN_MILLI_SECS = "httpTimeoutMilliSecs";
    String VMC_SRE_TENANT_IDENTIFIER = "vmc_sre";
    String RDC_TENANT_IDENTIFIER = "logiq-rdc";
    long QUEUE_POLL_TIMEOUT_SECS = 3L;
    long QUEUE_DEQUEUE_TIME_ELAPSED_SECS = 10L;
    String DISABLE_HTTP2_PROPERTY = "lemans.gatewayClient.disableHttp2";

    String HEADER_ARCTIC_SRE_ORG_ID = "arctic-sre-org-id";


    String INGESTION_GATEWAY_ACCESS_TOKEN = "ingestion-gateway-access-token";

    String INGESTION_GATEWAY_URL = "ingestion-gateway-url";

    String INGESTION_DISK_QUEUE_LOCATION = "ingestion-disk-queue-location";

    enum ForwarderType {
      LOG_FORWARDER
    }
}
