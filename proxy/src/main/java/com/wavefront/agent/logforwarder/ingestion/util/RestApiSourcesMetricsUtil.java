package com.wavefront.agent.logforwarder.ingestion.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.StructureFactory;
import io.dropwizard.metrics5.MetricRegistry;

public class RestApiSourcesMetricsUtil {

  public static MetricRegistry restApiMetricsRegistry = new MetricRegistry();

  // key=structure, value=set of source (ip's or hostnames)
  public static Map<StructureFactory.Structure, Set<String>> structureVsSources = new ConcurrentHashMap<>();

  /**
   * 1. update {@link #structureVsSources} map
   * 2. update rest-api source related metrics
   *
   * @param restApiFormat format of rest api
   * @param source        source ip or hostname, not null
   */
  public static void updateMessageReceivedFromSource(StructureFactory.Structure restApiFormat,
                                                     String source,
                                                     int numberOfMessages) {
    structureVsSources.putIfAbsent(restApiFormat, new HashSet<>());
    structureVsSources.get(restApiFormat).add(source);
    restApiMetricsRegistry
        .meter(getMeterName(restApiFormat, source))
        .mark(numberOfMessages);
  }

  public static String getMeterName(StructureFactory.Structure restApiFormat, String source) {
    return "rest-api-log-messages-received-per-source-" + restApiFormat + "-" + source;
  }
}