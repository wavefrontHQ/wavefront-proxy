package com.wavefront.agent.logforwarder.ingestion.client.gateway;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.filter.BackPressureFilter;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.filter.RateLimitFilter;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.metrics.MetricsManager;
import com.wavefront.agent.logforwarder.ingestion.http.client.ProxyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import io.dropwizard.metrics5.MetricRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;

/**
 * The class's responsibility is to manage the lifecycle, configuration of {@link GatewayClient} and
 * helps with all components with relevant configurations.
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/15/21 4:41 PM
 */
public class GatewayClientManager {
  public static final String LEMANS_CLIENT_PREFIX = "lemans.client.";
  public static final String PROPERTY_METRICS_LOGGING_INTERVAL_MINS = "lemans.client.metricsLoggingIntervalMinutes";
  public static final String PROPERTY_MAINTENANCE_INTERVAL_MICROS = "lemans.client.maintenanceIntervalMicros";
  private static final Logger logger = LoggerFactory.getLogger(GatewayClientManager.class);
  private static final int DEFAULT_SCHEDULER_THREAD_POOL_SIZE = 1;
  private static final int DEFAULT_METRICS_LOGGING_INTERVAL_METRICS = 5;
  public static final long DEFAULT_MAINTENANCE_INTERVAL_MICROS;
  private final String lemansAccessKey;
  private final URI lemansServiceUri;
  private final String lemansDataVolume;
  private final String agentType;
  private final Double agentVersion;
  private final String lemansTelemetryStreamName;
  private final boolean registerAgent;
  private final boolean enableTelemetry;
  private final Integer metricsLoggingIntervalMinutes;
  private final HashMap<String, String> agentProperties;
  private final MetricsManager metricsManager;
  private final URI hostServiceUri;
  private String agentId;
  private ProxyConfiguration proxyConfig;
  private GatewayClient gatewayClient;// ingestion into logs gateway
  private BackPressureFilter backPressureFilter;
  private RateLimitFilter rateLimitFilter;
  private Vertx vertx;

  private GatewayClientManager(String lemansAccessKey, URI lemansServiceUri, String lemansDataVolume,
                               String agentType, Double agentVersion, String lemansTelemetryStreamName,
                               boolean registerAgent, boolean enableTelemetry,
                               Integer metricsLoggingIntervalMinutes, HashMap<String, String> agentProperties,
                               URI hostServiceUri, MetricsManager metricsManager) {
    if (lemansAccessKey == null) {
      String errorMsg = "lemansAccessKey must be set";
      throw new IllegalArgumentException(errorMsg);
    }

    if (lemansServiceUri == null) {
      throw new IllegalArgumentException("lemansServiceUri must be set");
    } else if (lemansDataVolume == null) {
      throw new IllegalArgumentException("lemansDataVolume must be set");
    } else {
      if (agentVersion == null) {
        throw new IllegalArgumentException("agentVersion must be set for command channel");
      }

      if (hostServiceUri == null) {
        throw new IllegalArgumentException("hostServiceUri must be set for command channel");
      }
    }

    if (enableTelemetry && lemansTelemetryStreamName == null) {
      throw new IllegalArgumentException("lemansTelemetryStreamName must be set");
    } else {
      this.lemansAccessKey = lemansAccessKey;
      this.lemansServiceUri = lemansServiceUri;
      this.lemansDataVolume = lemansDataVolume;
      this.agentType = agentType;
      this.agentVersion = agentVersion;
      this.agentProperties = agentProperties;
      this.registerAgent = registerAgent;
      this.enableTelemetry = enableTelemetry;
      this.lemansTelemetryStreamName = lemansTelemetryStreamName;
      this.metricsLoggingIntervalMinutes = metricsLoggingIntervalMinutes;
      this.metricsManager = metricsManager;
      this.hostServiceUri = hostServiceUri;
    }
  }


  public void start() throws Throwable {
    logger.info("Starting Log Ingestion Gateway client...");
    this.vertx = Vertx.vertx();
    this.proxyConfig = ProxyConfiguration.defaultConfig();
    this.proxyConfig.getHosts().add(this.lemansServiceUri.getHost());
    logger.info("Successfully configured ServiceClient");
    this.gatewayClient = new GatewayClient(this.lemansServiceUri,  this.lemansAccessKey,
        this.metricsManager.metricRegistry, this.vertx);
    logger.info("Successfully created GatewayClient instance");
    this.rateLimitFilter = new RateLimitFilter(this.metricsManager.metricRegistry, this.agentId);
    this.gatewayClient.addGatewayClientFilter(this.rateLimitFilter);
    logger.info("Successfully initialized RateLimitFilter");
    if (this.lemansDataVolume != null) {
      Path queuePath = Paths.get(this.lemansDataVolume).resolve("backpressure-queue");
      if (!Files.exists(queuePath, new LinkOption[0])) {
        Files.createDirectories(queuePath);
      }
      logger.info("Successfully created log forwarder backpressure-queue on disk.");
      this.backPressureFilter = new BackPressureFilter(this.gatewayClient, queuePath.toString(), this.metricsManager.metricRegistry);
      this.gatewayClient.addGatewayClientFilter(this.backPressureFilter);
      logger.info("Successfully initialized BackPressureFilter with location {}", queuePath.toString());
    }

    //TODO Metrics figure out plan here
//    if (this.enableTelemetry) {
//      MetricsExporter clientMetricsExporter = new ClientMetricsExporter(this.gatewayClient, this.lemansTelemetryStreamName);
//      this.scheduledReporter = HttpReporter.forRegistry(this.metricsManager.metricRegistry).withMetricsExporter(clientMetricsExporter).build();
//      this.scheduledReporter.start(30L, TimeUnit.SECONDS);
//      logger.info("Starting HttpReporter with [telemtry stream = {}] , reporting every {} seconds", this.lemansTelemetryStreamName, 30);
//    }
//
//    if (this.metricsLoggingIntervalMinutes != null && this.metricsLoggingIntervalMinutes > 0) {
//      this.metricsManager.startConsoleReporter(this.metricsLoggingIntervalMinutes);
//      logger.info("Started dropwizard ConsoleReporter to log metrics to stdout every {} minutes", this.metricsLoggingIntervalMinutes);
//    }
    // TODO No plans to use these SaaS to log forwarder command listening. Remove before merge
//    if (this.enableLemansCommandChannel) {
//      if (this.commandExecutor == null) {
//        logger.info("Custom Command Executor instance is null using Synchronous Executor");
//        ServiceRequestSender requestSender = ServiceClientProvider.getServiceClient(this.proxyConfig);
//        this.commandExecutor = new SynchronousExecutor((String) null, this.hostServiceUri, requestSender);
//      }

//      logger.info("Successfully created Command Executor instance");
//      this.commandWatcher = new CommandWatcher(this.lemansServiceUri, this.otkClient, this.commandExecutor, this.metricsManager.metricRegistry, this.serviceClient);
//      logger.info("Successfully created CommandWatcher instance");
//      this.sessionClient = new SessionClient(this.lemansServiceUri, this.otkClient, this.commandExecutor, this.commandWatcher, this.serviceClient, this.metricsManager.metricRegistry, this.enableNewSessionClients);
//      logger.info("Successfully created SessionClient instance");
//      if (this.messageConsumer != null) {
//        this.internalMessageConsumer = new MessageConsumer(this.lemansServiceUri, this.metricsManager.metricRegistry, this.otkClient, this.messageConsumer, this.serviceClient, this.enableNewSessionClients);
//        logger.info("Successfully created MessageConsumer instance");
//      }
//
//    }
  }

  public URI getLemansServiceUri() {
    return this.lemansServiceUri;
  }

  public MetricRegistry getMetricRegistry() {
    return this.metricsManager.metricRegistry;
  }

  public String getAgentId() {
    return this.agentId;
  }

  public GatewayClient getGatewayClient() {
    return this.gatewayClient;
  }

  protected BackPressureFilter getBackPressureFilter() {
    return this.backPressureFilter;
  }

  public void stop() {
    if (this.backPressureFilter != null) {
      this.backPressureFilter.stop();
    }
  }

static {
    DEFAULT_MAINTENANCE_INTERVAL_MICROS=TimeUnit.SECONDS.toMicros(1L);
    DatabindCodec.mapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    DatabindCodec.mapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);
    DatabindCodec.mapper().configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES,true);
    }

public static class Builder {
  private final String lemansAccessKey;
  private final URI lemansServiceUri;
  private final String lemansDataVolume;
  private String agentType;
  private Double agentVersion;
  private String lemansTelemetryStreamName = "lemans-telemetry";
  private boolean registerAgent = true;
  private boolean enableTelemetry = true;
  private URI hostServiceUri;
  private Integer metricsLoggingIntervalMinutes = Integer.getInteger("lemans.client.metricsLoggingIntervalMinutes", 5);
  private HashMap<String, String> agentProperties = new HashMap();
  private MetricsManager metricsManager = new MetricsManager(Collections.emptyMap());

  private Builder(String lemansAccessKey, URI lemansServiceUri, String lemansDataVolume) {
    this.lemansAccessKey = lemansAccessKey;
    this.lemansServiceUri = lemansServiceUri;
    this.lemansDataVolume = lemansDataVolume;
  }

  public static Builder forConfig(String lemansAccessKey, URI lemansServiceUri, String lemansDataVolume) {
    return new Builder(lemansAccessKey, lemansServiceUri, lemansDataVolume);
  }

  public Builder withAgentType(String agentType) {
    this.agentType = agentType;
    return this;
  }

  public Builder withAgentVersion(Double agentVersion) {
    this.agentVersion = agentVersion;
    return this;
  }

  public Builder withAgentProperties(HashMap<String, String> agentProperties) {
    this.agentProperties = agentProperties;
    return this;
  }

  public Builder shouldRegisterAgent(boolean registerAgent) {
    this.registerAgent = registerAgent;
    return this;
  }

  public Builder shouldEnableTelemetry(boolean enableTelmetry) {
    this.enableTelemetry = enableTelmetry;
    return this;
  }

  public Builder withTelemetryStreamName(String lemansTelemetryStreamName) {
    this.lemansTelemetryStreamName = lemansTelemetryStreamName;
    return this;
  }

  public Builder withMetricsLoggingIntervalMinutes(Integer metricsLoggingIntervalMinutes) {
    this.metricsLoggingIntervalMinutes = metricsLoggingIntervalMinutes;
    return this;
  }

  Builder withMetricsManager(MetricsManager metricsManager) {
    this.metricsManager = metricsManager;
    return this;
  }

  public Builder withHostServiceUri(URI hostServiceUri) {
    this.hostServiceUri = hostServiceUri;
    return this;
  }

  public GatewayClientManager build() {
    return new GatewayClientManager(this.lemansAccessKey, this.lemansServiceUri, this.lemansDataVolume,
        this.agentType, this.agentVersion, this.lemansTelemetryStreamName, this.registerAgent,
        this.enableTelemetry, this.metricsLoggingIntervalMinutes, this.agentProperties,
        this.hostServiceUri, this.metricsManager);
  }
}
}
