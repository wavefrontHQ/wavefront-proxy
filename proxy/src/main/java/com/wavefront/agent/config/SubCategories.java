package com.wavefront.agent.config;

import com.fasterxml.jackson.annotation.JsonValue;

public enum SubCategories {
  METRICS("Metrics", 1),
  LOGS("Logs", 2),
  HISTO("Histograms", 3),
  TRACES("Traces", 4),
  EVENTS("Events", 5),
  SOURCETAGS("Source Tags", 6),
  OTHER("Other", 6),
  MEMORY("Disk buffer", 1),
  DISK("Disk buffer", 2),
  GRAPHITE("Graphite", 5),
  JSON("JSON", 6),
  DDOG("DataDog", 7),
  TLS("HTTPS TLS", 1),
  CORS("CORS", 2),
  SQS("External SQS", 3),
  CONF("Configuration", 0),
  HTTPPROXY("HTTP/S Proxy", 3),
  NA("Others", 9999),
  OPENTEL("Open Telemetry", 8),
  FILEB("Filebeat logs", 10),
  RAWLOGS("Raw logs", 11),
  TSDB("OpenTSDB", 12),
  TRACES_JAEGER("Jaeger", 13),
  TRACES_ZIPKIN("Zipkin", 14); // for hided options

  private final String value;
  private int order;

  SubCategories(String value, int order) {
    this.value = value;
    this.order = order;
  }

  @JsonValue
  public String getValue() {
    return value;
  }

  public int getOrder() {
    return order;
  }
}
