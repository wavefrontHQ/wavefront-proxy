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
    String LI_AGENT_STATUS_RESPONSE_MSG = "{\"status\":\"ok\"}";
    /* added for debugging VLL-130 on an vmc sddc */
    String HOST_NAME = "source_hostname";
    String HOST_STRING = "source_hoststring";
    String COMPONENT = "component";
    String SDDC_ID = "sddc_id";
    String FORWARDER_ID = "forwarder_id";
    String ORG_ID = "org_id";
    String REGION = "region";
    String ORG_TYPE = "org_type";
    String SDDC_ENV = "sddc_env";
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
    String BUILD_NUMBER = "build.number";
    String SYSLOG_HOST = "syslogHost";
    String SYSLOG_PORT = "syslogPort";
    String DISABLE_HTTP2_PROPERTY = "lemans.gatewayClient.disableHttp2";

    String HEALTH_SUCCESS_API_RESPONSE = "Connected to vRealize Log Insight Cloud";
    String HEALTH_ERROR_API_RESPONSE = "Unable to connect to vRealize Log Insight Cloud";

    String HEADER_ARCTIC_SRE_ORG_ID = "arctic-sre-org-id";

    String DIMENSION_MSP_MASTER_ORG_ID = "dimension-msp-master-org-id";

    // Time greater than http request timeout of 60secs.
    long LATCH_TIMEOUT_SECS = 65;

    enum ForwarderType {
      LOG_FORWARDER
    }
}
