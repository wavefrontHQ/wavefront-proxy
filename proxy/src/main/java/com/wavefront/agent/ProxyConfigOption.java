package com.wavefront.agent;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

enum Categories {
  GENERAL("General", 1),
  INPUT("Input", 2),
  BUFFER("Buffering", 3),
  OUTPUT("Output", 4),
  TRACE("Trace", 5),
  NA("", 0); // for hided options

  private final String value;
  private int order;

  Categories(String value, int order) {
    this.value = value;
    this.order = order;
  }

  public String getValue() {
    return value;
  }

  public int getOrder() {
    return order;
  }
}

enum SubCategories {
  METRICS("Metrics", 1),
  LOGS("Logs", 2),
  HISTO("Histograms", 3),
  TRACES("Traces", 4),
  EVENTS("Events", 5),
  SOURCETAGS("Source Tags", 5),
  NA("", 0),
  OTHER("Other", 6),
  MEMORY("Disk buffer", 1),
  DISK("Disk buffer", 2),
  GRAPHITE("Graphite", 5), JSON("JSON", 6), DDOG("DataDog", 7), TLS("HTTPS TLS", 1), CORS("CORS", 2), SQS("External SQS", 3), CONF("Configuration", 0), HTTPPROXY("HTTP/S Proxy", 3); // for hided options

  private final String value;
  private int order;

  SubCategories(String value, int order) {
    this.value = value;
    this.order = order;
  }

  public String getValue() {
    return value;
  }

  public int getOrder() {
    return order;
  }
}

@Retention(RUNTIME)
@Target({FIELD})
public @interface ProxyConfigOption {
  Categories category();

  SubCategories subCategory();

  boolean hide() default false;
}
