package com.wavefront.agent.logforwarder.ingestion.constants;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 8/30/21 4:40 PM
 */
public interface IngestConstants {
  String TENANT_ID_HEADER = "lm-tenant-id";
  String BILLABLE_HEADER = "billable";
  String STRUCTURE_HEADER = "structure";
  String FORMAT_HEADER = "format";
  String CHAIN_HEADER = "chain";
  String AGENT_HEADER = "agent";
  String AUTH_HEADER = "Authorization";
  String EVENT_TYPE_FIELD = "event_type";
  String VMC_ORG_ID = "vmc-org-id";
  String SDDC_ID = "sddc_id";
  String AGENTID_FIELD = "agentId";
  String LOGS_FIELD = "logs";
  String RETRY_COUNT_FIELD = "retryCount";
  String KAFKA_RETRY_FLAG = "kafka-retry-flag";
  String ORG_ID_HEADER = "lm-org-id";
  String LOG_TYPE = "log_type";
  String COMPONENT = "component";
  String PARSE_SYSLOG = "parse_syslog";
  String SYSLOG_PARSED = "lint_syslog_parsed";
}