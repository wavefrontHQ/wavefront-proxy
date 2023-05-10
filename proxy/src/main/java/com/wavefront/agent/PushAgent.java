package com.wavefront.agent;

import static com.google.common.base.Preconditions.checkArgument;
import static com.wavefront.agent.ProxyContext.entityPropertiesFactoryMap;
import static com.wavefront.agent.ProxyContext.queuesManager;
import static com.wavefront.agent.ProxyUtil.createInitializer;
import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;
import static com.wavefront.agent.data.EntityProperties.NO_RATE_LIMIT;
import static com.wavefront.common.Utils.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tdunning.math.stats.AgentDigest;
import com.tdunning.math.stats.AgentDigest.AgentDigestMarshaller;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.channels.Connection;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.auth.TokenAuthenticatorBuilder;
import com.wavefront.agent.channel.CachingHostnameLookupResolver;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.channel.HealthCheckManagerImpl;
import com.wavefront.agent.channel.SharedGraphiteHostAnnotator;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.buffers.BuffersManagerConfig;
import com.wavefront.agent.core.handlers.*;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueuesManagerDefault;
import com.wavefront.agent.core.senders.SenderTasksManager;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.agent.data.EntityRateLimiter;
import com.wavefront.agent.formatter.GraphiteFormatter;
import com.wavefront.agent.histogram.*;
import com.wavefront.agent.histogram.HistogramUtils.HistogramKeyMarshaller;
import com.wavefront.agent.histogram.accumulator.AccumulationCache;
import com.wavefront.agent.histogram.accumulator.Accumulator;
import com.wavefront.agent.histogram.accumulator.AgentDigestFactory;
import com.wavefront.agent.listeners.*;
import com.wavefront.agent.listeners.otlp.OtlpGrpcMetricsHandler;
import com.wavefront.agent.listeners.otlp.OtlpGrpcTraceHandler;
import com.wavefront.agent.listeners.otlp.OtlpHttpHandler;
import com.wavefront.agent.listeners.tracing.*;
import com.wavefront.agent.logsharvesting.FilebeatIngester;
import com.wavefront.agent.logsharvesting.LogsIngester;
import com.wavefront.agent.preprocessor.PreprocessorRuleMetrics;
import com.wavefront.agent.preprocessor.ReportPointAddPrefixTransformer;
import com.wavefront.agent.preprocessor.SpanSanitizeTransformer;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.agent.sampler.SpanSamplerUtils;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.*;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.entities.tracing.sampling.CompositeSampler;
import com.wavefront.sdk.entities.tracing.sampling.RateSampler;
import com.wavefront.sdk.entities.tracing.sampling.Sampler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.ssl.SslContext;
import java.io.File;
import java.net.BindException;
import java.net.InetAddress;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import net.openhft.chronicle.map.ChronicleMap;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.logstash.beats.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

/** Push-only Agent. */
public class PushAgent extends AbstractAgent {
  private static final Logger logger = LoggerFactory.getLogger(PushAgent.class.getCanonicalName());
  public static final Logger stats = LoggerFactory.getLogger("stats");

  public static boolean isMulticastingActive;

  protected final Map<Integer, Thread> listeners = new HashMap<>();

  protected final IdentityHashMap<ChannelOption<?>, Object> childChannelOptions =
      new IdentityHashMap<>();
  protected final Counter bindErrors =
      Metrics.newCounter(ExpectedAgentMetric.LISTENERS_BIND_ERRORS.metricName);
  protected final Supplier<Map<ReportableEntityType, ReportableEntityDecoder<?, ?>>>
      decoderSupplier =
          lazySupplier(
              () ->
                  ImmutableMap.<ReportableEntityType, ReportableEntityDecoder<?, ?>>builder()
                      .put(
                          ReportableEntityType.POINT,
                          new ReportPointDecoder(
                              () -> "unknown", proxyConfig.getCustomSourceTags()))
                      .put(ReportableEntityType.SOURCE_TAG, new ReportSourceTagDecoder())
                      .put(
                          ReportableEntityType.HISTOGRAM,
                          new ReportPointDecoderWrapper(new HistogramDecoder("unknown")))
                      .put(ReportableEntityType.TRACE, new SpanDecoder("unknown"))
                      .put(ReportableEntityType.TRACE_SPAN_LOGS, new SpanLogsDecoder())
                      .put(ReportableEntityType.EVENT, new EventDecoder())
                      .put(
                          ReportableEntityType.LOGS,
                          new ReportLogDecoder(
                              () -> "unknown",
                              proxyConfig.getCustomSourceTags(),
                              proxyConfig.getCustomTimestampTags(),
                              proxyConfig.getCustomMessageTags(),
                              proxyConfig.getCustomApplicationTags(),
                              proxyConfig.getCustomServiceTags(),
                              proxyConfig.getCustomLevelTags(),
                              proxyConfig.getCustomExceptionTags()))
                      .build());
  // default rate sampler which always samples.
  protected final RateSampler rateSampler = new RateSampler(1.0d);
  protected ScheduledExecutorService histogramExecutor;
  protected ScheduledExecutorService histogramFlushExecutor;
  @VisibleForTesting protected final List<Runnable> histogramFlushRunnables = new ArrayList<>();
  protected SharedGraphiteHostAnnotator remoteHostAnnotator;
  protected Function<InetAddress, String> hostnameResolver;
  protected Function<Histogram, Histogram> histogramRecompressor = null;
  protected ReportableEntityHandlerFactoryImpl handlerFactory;
  protected ReportableEntityHandlerFactory deltaCounterHandlerFactory;
  protected HealthCheckManager healthCheckManager;
  protected TokenAuthenticator tokenAuthenticator = TokenAuthenticator.DUMMY_AUTHENTICATOR;
  private Logger blockedPointsLogger;
  private Logger blockedHistogramsLogger;
  private Logger blockedSpansLogger;
  private Logger blockedLogsLogger;

  public static void main(String[] args) {
    // Start the ssh daemon
    String versionStr =
        "Wavefront Proxy version "
            + getBuildVersion()
            + " (pkg:"
            + getPackage()
            + ")"
            + ", runtime: "
            + getJavaVersion();
    logger.info(versionStr);
    new PushAgent().start(args);
  }

  @Override
  protected void startListeners() throws Exception {

    isMulticastingActive = proxyConfig.getMulticastingTenantList().size() > 0;
    ProxyContext.queuesManager = new QueuesManagerDefault(proxyConfig);
    SenderTasksManager.init(apiContainer, agentId);

    /***** Setup Buffers *****/

    BuffersManagerConfig cfg = new BuffersManagerConfig();

    double maxMemory = Runtime.getRuntime().maxMemory();
    double buffersMaxMemory = Math.min(maxMemory / 2, 1_000_000_000);

    cfg.memoryCfg.msgExpirationTime = proxyConfig.getMemoryBufferExpirationTime();
    if (cfg.memoryCfg.msgExpirationTime != -1) {
      cfg.memoryCfg.msgExpirationTime *= 1000;
    }
    cfg.memoryCfg.msgRetry = proxyConfig.getMemoryBufferRetryLimit();
    cfg.memoryCfg.maxMemory = (long) buffersMaxMemory;

    cfg.disk = !proxyConfig.getDisableBuffer();
    if (cfg.disk) {
      cfg.diskCfg.buffer = new File(proxyConfig.getBufferFile());
      cfg.memoryCfg.maxMemory = (long) (buffersMaxMemory * 0.75);
      cfg.diskCfg.maxMemory = (long) (buffersMaxMemory * 0.25);
      cfg.diskCfg.validate();
    }

    cfg.external = proxyConfig.isSqsQueueBuffer();
    if (cfg.external) {
      cfg.sqsCfg.template = proxyConfig.getSqsQueueNameTemplate();
      cfg.sqsCfg.region = proxyConfig.getSqsQueueRegion();
      cfg.sqsCfg.id = proxyConfig.getSqsQueueIdentifier();
      cfg.sqsCfg.validate();
    }

    BuffersManager.init(cfg);

    /***** END Setup Buffers *****/

    blockedPointsLogger = LoggerFactory.getLogger(proxyConfig.getBlockedPointsLoggerName());
    blockedHistogramsLogger = LoggerFactory.getLogger(proxyConfig.getBlockedHistogramsLoggerName());
    blockedSpansLogger = LoggerFactory.getLogger(proxyConfig.getBlockedSpansLoggerName());
    blockedLogsLogger = LoggerFactory.getLogger(proxyConfig.getBlockedLogsLoggerName());

    if (proxyConfig.getSoLingerTime() >= 0) {
      childChannelOptions.put(ChannelOption.SO_LINGER, proxyConfig.getSoLingerTime());
    }
    hostnameResolver =
        new CachingHostnameLookupResolver(
            proxyConfig.isDisableRdnsLookup(), ExpectedAgentMetric.RDNS_CACHE_SIZE.metricName);

    remoteHostAnnotator =
        new SharedGraphiteHostAnnotator(proxyConfig.getCustomSourceTags(), hostnameResolver);
    // MONIT-25479: when multicasting histogram, use the central cluster histogram
    // accuracy
    if (proxyConfig.isHistogramPassthroughRecompression()) {
      histogramRecompressor =
          new HistogramRecompressor(
              () ->
                  entityPropertiesFactoryMap
                      .get(CENTRAL_TENANT_NAME)
                      .getGlobalProperties()
                      .getHistogramStorageAccuracy());
    }
    handlerFactory =
        new ReportableEntityHandlerFactoryImpl(
            validationConfiguration,
            blockedPointsLogger,
            blockedHistogramsLogger,
            blockedSpansLogger,
            histogramRecompressor,
            blockedLogsLogger);
    healthCheckManager = new HealthCheckManagerImpl(proxyConfig);
    tokenAuthenticator = configureTokenAuthenticator();

    SpanSampler spanSampler = createSpanSampler();

    if (proxyConfig.getAdminApiListenerPort() > 0) {
      startAdminListener(proxyConfig.getAdminApiListenerPort());
    }

    csvToList(proxyConfig.getHttpHealthCheckPorts())
        .forEach(port -> startHealthCheckListener(port));

    csvToList(proxyConfig.getPushListenerPorts())
        .forEach(
            port -> {
              startGraphiteListener(port, handlerFactory, remoteHostAnnotator, spanSampler);
              logger.info("listening on port: " + port + " for Wavefront metrics");
            });

    csvToList(proxyConfig.getDeltaCountersAggregationListenerPorts())
        .forEach(
            port -> {
              startDeltaCounterListener(port, remoteHostAnnotator, spanSampler);
              logger.info("listening on port: " + port + " for Wavefront delta counter metrics");
            });

    bootstrapHistograms(spanSampler);

    if (StringUtils.isNotBlank(proxyConfig.getGraphitePorts())
        || StringUtils.isNotBlank(proxyConfig.getPicklePorts())) {
      if (tokenAuthenticator.authRequired()) {
        logger.warn("Graphite mode is not compatible with HTTP authentication, ignoring");
      } else {
        Preconditions.checkNotNull(
            proxyConfig.getGraphiteFormat(),
            "graphiteFormat must be supplied to enable graphite support");
        Preconditions.checkNotNull(
            proxyConfig.getGraphiteDelimiters(),
            "graphiteDelimiters must be supplied to enable graphite support");
        GraphiteFormatter graphiteFormatter =
            new GraphiteFormatter(
                proxyConfig.getGraphiteFormat(),
                proxyConfig.getGraphiteDelimiters(),
                proxyConfig.getGraphiteFieldsToRemove());
        csvToList(proxyConfig.getGraphitePorts())
            .forEach(
                port -> {
                  preprocessors
                      .getSystemPreprocessor(port)
                      .forPointLine()
                      .addTransformer(0, graphiteFormatter);
                  startGraphiteListener(port, handlerFactory, null, spanSampler);
                  logger.info("listening on port: " + port + " for graphite metrics");
                });
        csvToList(proxyConfig.getPicklePorts())
            .forEach(port -> startPickleListener(port, handlerFactory, graphiteFormatter));
      }
    }

    csvToList(proxyConfig.getOpentsdbPorts())
        .forEach(port -> startOpenTsdbListener(port, handlerFactory));

    if (proxyConfig.getDataDogJsonPorts() != null) {
      HttpClient httpClient =
          HttpClientBuilder.create()
              .useSystemProperties()
              .setUserAgent(proxyConfig.getHttpUserAgent())
              .setConnectionTimeToLive(1, TimeUnit.MINUTES)
              .setMaxConnPerRoute(100)
              .setMaxConnTotal(100)
              .setRetryHandler(
                  new DefaultHttpRequestRetryHandler(proxyConfig.getHttpAutoRetries(), true))
              .setDefaultRequestConfig(
                  RequestConfig.custom()
                      .setContentCompressionEnabled(true)
                      .setRedirectsEnabled(true)
                      .setConnectTimeout(proxyConfig.getHttpConnectTimeout())
                      .setConnectionRequestTimeout(proxyConfig.getHttpConnectTimeout())
                      .setSocketTimeout(proxyConfig.getHttpRequestTimeout())
                      .build())
              .build();

      csvToList(proxyConfig.getDataDogJsonPorts())
          .forEach(port -> startDataDogListener(port, handlerFactory, httpClient));
    }

    startDistributedTracingListeners(spanSampler);

    startOtlpListeners(spanSampler);

    csvToList(proxyConfig.getPushRelayListenerPorts())
        .forEach(port -> startRelayListener(port, handlerFactory, remoteHostAnnotator));
    csvToList(proxyConfig.getJsonListenerPorts())
        .forEach(port -> startJsonListener(port, handlerFactory));
    csvToList(proxyConfig.getWriteHttpJsonListenerPorts())
        .forEach(port -> startWriteHttpJsonListener(port, handlerFactory));

    // Logs ingestion.
    if (proxyConfig.getFilebeatPort() > 0 || proxyConfig.getRawLogsPort() > 0) {
      if (loadLogsIngestionConfig() != null) {
        logger.info("Initializing logs ingestion");
        try {
          final LogsIngester logsIngester =
              new LogsIngester(
                  handlerFactory, this::loadLogsIngestionConfig, proxyConfig.getPrefix());
          logsIngester.start();

          if (proxyConfig.getFilebeatPort() > 0) {
            startLogsIngestionListener(proxyConfig.getFilebeatPort(), logsIngester);
          }
          if (proxyConfig.getRawLogsPort() > 0) {
            startRawLogsIngestionListener(proxyConfig.getRawLogsPort(), logsIngester);
          }
        } catch (ConfigurationException e) {
          logger.error("Cannot start logsIngestion", e);
        }
      } else {
        logger.warn("Cannot start logsIngestion: invalid configuration or no config specified");
      }
    }
  }

  private void startDistributedTracingListeners(SpanSampler spanSampler) {
    csvToList(proxyConfig.getTraceListenerPorts())
        .forEach(port -> startTraceListener(port, handlerFactory, spanSampler));
    csvToList(proxyConfig.getCustomTracingListenerPorts())
        .forEach(
            port ->
                startCustomTracingListener(
                    port,
                    handlerFactory,
                    new InternalProxyWavefrontClient(handlerFactory, port),
                    spanSampler));
    csvToList(proxyConfig.getTraceJaegerListenerPorts())
        .forEach(
            port -> {
              PreprocessorRuleMetrics ruleMetrics =
                  new PreprocessorRuleMetrics(
                      Metrics.newCounter(
                          new TaggedMetricName(
                              "point.spanSanitize", "count", "port", String.valueOf(port))),
                      null,
                      null);
              preprocessors
                  .getSystemPreprocessor(port)
                  .forSpan()
                  .addTransformer(new SpanSanitizeTransformer(ruleMetrics));
              startTraceJaegerListener(
                  port,
                  handlerFactory,
                  new InternalProxyWavefrontClient(handlerFactory, port),
                  spanSampler);
            });

    csvToList(proxyConfig.getTraceJaegerGrpcListenerPorts())
        .forEach(
            port -> {
              PreprocessorRuleMetrics ruleMetrics =
                  new PreprocessorRuleMetrics(
                      Metrics.newCounter(
                          new TaggedMetricName(
                              "point.spanSanitize", "count", "port", String.valueOf(port))),
                      null,
                      null);
              preprocessors
                  .getSystemPreprocessor(port)
                  .forSpan()
                  .addTransformer(new SpanSanitizeTransformer(ruleMetrics));
              startTraceJaegerGrpcListener(
                  port,
                  handlerFactory,
                  new InternalProxyWavefrontClient(handlerFactory, port),
                  spanSampler);
            });
    csvToList(proxyConfig.getTraceJaegerHttpListenerPorts())
        .forEach(
            port -> {
              PreprocessorRuleMetrics ruleMetrics =
                  new PreprocessorRuleMetrics(
                      Metrics.newCounter(
                          new TaggedMetricName(
                              "point.spanSanitize", "count", "port", String.valueOf(port))),
                      null,
                      null);
              preprocessors
                  .getSystemPreprocessor(port)
                  .forSpan()
                  .addTransformer(new SpanSanitizeTransformer(ruleMetrics));
              startTraceJaegerHttpListener(
                  port,
                  handlerFactory,
                  new InternalProxyWavefrontClient(handlerFactory, port),
                  spanSampler);
            });
    csvToList(proxyConfig.getTraceZipkinListenerPorts())
        .forEach(
            port -> {
              PreprocessorRuleMetrics ruleMetrics =
                  new PreprocessorRuleMetrics(
                      Metrics.newCounter(
                          new TaggedMetricName(
                              "point.spanSanitize", "count", "port", String.valueOf(port))),
                      null,
                      null);
              preprocessors
                  .getSystemPreprocessor(port)
                  .forSpan()
                  .addTransformer(new SpanSanitizeTransformer(ruleMetrics));
              startTraceZipkinListener(
                  port,
                  handlerFactory,
                  new InternalProxyWavefrontClient(handlerFactory, port),
                  spanSampler);
            });
  }

  private void startOtlpListeners(SpanSampler spanSampler) {
    csvToList(proxyConfig.getOtlpGrpcListenerPorts())
        .forEach(
            port -> {
              PreprocessorRuleMetrics ruleMetrics =
                  new PreprocessorRuleMetrics(
                      Metrics.newCounter(
                          new TaggedMetricName(
                              "point.spanSanitize", "count", "port", String.valueOf(port))),
                      null,
                      null);
              preprocessors
                  .getSystemPreprocessor(port)
                  .forSpan()
                  .addTransformer(new SpanSanitizeTransformer(ruleMetrics));
              startOtlpGrpcListener(
                  port,
                  handlerFactory,
                  new InternalProxyWavefrontClient(handlerFactory, port),
                  spanSampler);
            });

    csvToList(proxyConfig.getOtlpHttpListenerPorts())
        .forEach(
            port -> {
              PreprocessorRuleMetrics ruleMetrics =
                  new PreprocessorRuleMetrics(
                      Metrics.newCounter(
                          new TaggedMetricName(
                              "point.spanSanitize", "count", "port", String.valueOf(port))),
                      null,
                      null);
              preprocessors
                  .getSystemPreprocessor(port)
                  .forSpan()
                  .addTransformer(new SpanSanitizeTransformer(ruleMetrics));
              startOtlpHttpListener(
                  port,
                  handlerFactory,
                  new InternalProxyWavefrontClient(handlerFactory, port),
                  spanSampler);
            });
  }

  private SpanSampler createSpanSampler() {
    rateSampler.setSamplingRate(
        entityPropertiesFactoryMap
            .get(CENTRAL_TENANT_NAME)
            .getGlobalProperties()
            .getTraceSamplingRate());
    Sampler durationSampler =
        SpanSamplerUtils.getDurationSampler(proxyConfig.getTraceSamplingDuration());
    List<Sampler> samplers = SpanSamplerUtils.fromSamplers(rateSampler, durationSampler);
    return new SpanSampler(
        new CompositeSampler(samplers),
        () ->
            entityPropertiesFactoryMap
                .get(CENTRAL_TENANT_NAME)
                .getGlobalProperties()
                .getActiveSpanSamplingPolicies());
  }

  private void bootstrapHistograms(SpanSampler spanSampler) throws Exception {
    List<Integer> histMinPorts = csvToList(proxyConfig.getHistogramMinuteListenerPorts());
    List<Integer> histHourPorts = csvToList(proxyConfig.getHistogramHourListenerPorts());
    List<Integer> histDayPorts = csvToList(proxyConfig.getHistogramDayListenerPorts());
    List<Integer> histDistPorts = csvToList(proxyConfig.getHistogramDistListenerPorts());

    int activeHistogramAggregationTypes =
        (histDayPorts.size() > 0 ? 1 : 0)
            + (histHourPorts.size() > 0 ? 1 : 0)
            + (histMinPorts.size() > 0 ? 1 : 0)
            + (histDistPorts.size() > 0 ? 1 : 0);
    if (activeHistogramAggregationTypes > 0) {
      /* Histograms enabled */
      histogramExecutor =
          Executors.newScheduledThreadPool(
              1 + activeHistogramAggregationTypes, new NamedThreadFactory("histogram-service"));
      histogramFlushExecutor =
          Executors.newScheduledThreadPool(
              Runtime.getRuntime().availableProcessors() / 2,
              new NamedThreadFactory("histogram-flush"));
      managedExecutors.add(histogramExecutor);
      managedExecutors.add(histogramFlushExecutor);

      File baseDirectory = new File(proxyConfig.getHistogramStateDirectory());

      // Central dispatch
      ReportableEntityHandler<ReportPoint> pointHandler =
          handlerFactory.getHandler(
              "histogram_ports", queuesManager.initQueue(ReportableEntityType.HISTOGRAM));

      startHistogramListeners(
          histMinPorts,
          pointHandler,
          remoteHostAnnotator,
          Granularity.MINUTE,
          proxyConfig.getHistogramMinuteFlushSecs(),
          proxyConfig.isHistogramMinuteMemoryCache(),
          baseDirectory,
          proxyConfig.getHistogramMinuteAccumulatorSize(),
          proxyConfig.getHistogramMinuteAvgKeyBytes(),
          proxyConfig.getHistogramMinuteAvgDigestBytes(),
          proxyConfig.getHistogramMinuteCompression(),
          proxyConfig.isHistogramMinuteAccumulatorPersisted(),
          spanSampler);
      startHistogramListeners(
          histHourPorts,
          pointHandler,
          remoteHostAnnotator,
          Granularity.HOUR,
          proxyConfig.getHistogramHourFlushSecs(),
          proxyConfig.isHistogramHourMemoryCache(),
          baseDirectory,
          proxyConfig.getHistogramHourAccumulatorSize(),
          proxyConfig.getHistogramHourAvgKeyBytes(),
          proxyConfig.getHistogramHourAvgDigestBytes(),
          proxyConfig.getHistogramHourCompression(),
          proxyConfig.isHistogramHourAccumulatorPersisted(),
          spanSampler);
      startHistogramListeners(
          histDayPorts,
          pointHandler,
          remoteHostAnnotator,
          Granularity.DAY,
          proxyConfig.getHistogramDayFlushSecs(),
          proxyConfig.isHistogramDayMemoryCache(),
          baseDirectory,
          proxyConfig.getHistogramDayAccumulatorSize(),
          proxyConfig.getHistogramDayAvgKeyBytes(),
          proxyConfig.getHistogramDayAvgDigestBytes(),
          proxyConfig.getHistogramDayCompression(),
          proxyConfig.isHistogramDayAccumulatorPersisted(),
          spanSampler);
      startHistogramListeners(
          histDistPorts,
          pointHandler,
          remoteHostAnnotator,
          null,
          proxyConfig.getHistogramDistFlushSecs(),
          proxyConfig.isHistogramDistMemoryCache(),
          baseDirectory,
          proxyConfig.getHistogramDistAccumulatorSize(),
          proxyConfig.getHistogramDistAvgKeyBytes(),
          proxyConfig.getHistogramDistAvgDigestBytes(),
          proxyConfig.getHistogramDistCompression(),
          proxyConfig.isHistogramDistAccumulatorPersisted(),
          spanSampler);
    }
  }

  @Nullable
  protected SslContext getSslContext(int port) {
    return (secureAllPorts || tlsPorts.contains(port)) ? sslContext : null;
  }

  @Nullable
  protected CorsConfig getCorsConfig(int port) {
    List<String> ports = proxyConfig.getCorsEnabledPorts();
    List<String> corsOrigin = proxyConfig.getCorsOrigin();
    if (ports.equals(ImmutableList.of("*")) || ports.contains(String.valueOf(port))) {
      CorsConfigBuilder builder;
      if (corsOrigin.equals(ImmutableList.of("*"))) {
        builder = CorsConfigBuilder.forOrigin(corsOrigin.get(0));
      } else {
        builder = CorsConfigBuilder.forOrigins(corsOrigin.toArray(new String[0]));
      }
      builder.allowedRequestHeaders("Content-Type", "Referer", "User-Agent");
      builder.allowedRequestMethods(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT);
      if (proxyConfig.isCorsAllowNullOrigin()) {
        builder.allowNullOrigin();
      }
      return builder.build();
    } else {
      return null;
    }
  }

  protected void startJsonListener(int port, ReportableEntityHandlerFactory handlerFactory) {
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);

    ChannelHandler channelHandler =
        new JsonMetricsPortUnificationHandler(
            port,
            tokenAuthenticator,
            healthCheckManager,
            handlerFactory,
            proxyConfig.getPrefix(),
            proxyConfig.getHostname(),
            preprocessors.get(port));

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getPushListenerMaxReceivedLength(),
                    proxyConfig.getPushListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-plaintext-json-" + port);
    logger.info("listening on port: " + port + " for JSON metrics data");
  }

  protected void startWriteHttpJsonListener(
      int port, ReportableEntityHandlerFactory handlerFactory) {
    registerPrefixFilter(port);
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);

    ChannelHandler channelHandler =
        new WriteHttpJsonPortUnificationHandler(
            port,
            tokenAuthenticator,
            healthCheckManager,
            handlerFactory,
            proxyConfig.getHostname(),
            preprocessors.get(port));

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getPushListenerMaxReceivedLength(),
                    proxyConfig.getPushListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-plaintext-writehttpjson-" + port);
    logger.info("listening on port: " + port + " for write_http data");
  }

  protected void startOpenTsdbListener(
      final int port, ReportableEntityHandlerFactory handlerFactory) {
    registerPrefixFilter(port);
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);

    ReportableEntityDecoder<String, ReportPoint> openTSDBDecoder =
        new ReportPointDecoderWrapper(
            new OpenTSDBDecoder("unknown", proxyConfig.getCustomSourceTags()));

    ChannelHandler channelHandler =
        new OpenTSDBPortUnificationHandler(
            port,
            tokenAuthenticator,
            healthCheckManager,
            openTSDBDecoder,
            handlerFactory,
            preprocessors.get(port),
            hostnameResolver);

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getPushListenerMaxReceivedLength(),
                    proxyConfig.getPushListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-plaintext-opentsdb-" + port);
    logger.info("listening on port: " + port + " for OpenTSDB metrics");
  }

  protected void startDataDogListener(
      final int port, ReportableEntityHandlerFactory handlerFactory, HttpClient httpClient) {
    if (tokenAuthenticator.authRequired()) {
      logger.warn(
          "Port: " + port + " (DataDog) is not compatible with HTTP authentication, ignoring");
      return;
    }
    registerPrefixFilter(port);
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);

    ChannelHandler channelHandler =
        new DataDogPortUnificationHandler(
            port,
            healthCheckManager,
            handlerFactory,
            proxyConfig.getDataDogRequestRelayAsyncThreads(),
            proxyConfig.isDataDogRequestRelaySyncMode(),
            proxyConfig.isDataDogProcessSystemMetrics(),
            proxyConfig.isDataDogProcessServiceChecks(),
            httpClient,
            proxyConfig.getDataDogRequestRelayTarget(),
            preprocessors.get(port));

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getPushListenerMaxReceivedLength(),
                    proxyConfig.getPushListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-plaintext-datadog-" + port);
    logger.info("listening on port: " + port + " for DataDog metrics");
  }

  protected void startPickleListener(
      int port, ReportableEntityHandlerFactory handlerFactory, GraphiteFormatter formatter) {
    if (tokenAuthenticator.authRequired()) {
      logger.warn(
          "Port: "
              + port
              + " (pickle format) is not compatible with HTTP authentication, ignoring");
      return;
    }
    registerPrefixFilter(port);

    // Set up a custom handler
    ChannelHandler channelHandler =
        new ChannelByteArrayHandler(
            new PickleProtocolDecoder(
                "unknown", proxyConfig.getCustomSourceTags(), formatter.getMetricMangler(), port),
            handlerFactory.getHandler(port, queuesManager.initQueue(ReportableEntityType.POINT)),
            preprocessors.get(port),
            blockedPointsLogger);

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    ImmutableList.of(
                        () ->
                            new LengthFieldBasedFrameDecoder(
                                ByteOrder.BIG_ENDIAN, 1000000, 0, 4, 0, 4, false),
                        ByteArrayDecoder::new,
                        () -> channelHandler),
                    port,
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-binary-pickle-" + port);
    logger.info("listening on port: " + port + " for Graphite/pickle protocol metrics");
  }

  protected void startTraceListener(
      final int port, ReportableEntityHandlerFactory handlerFactory, SpanSampler sampler) {
    registerPrefixFilter(port);
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);

    ChannelHandler channelHandler =
        new TracePortUnificationHandler(
            port,
            tokenAuthenticator,
            healthCheckManager,
            new SpanDecoder("unknown"),
            new SpanLogsDecoder(),
            preprocessors.get(port),
            handlerFactory,
            sampler,
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE)
                    .isFeatureDisabled(),
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE_SPAN_LOGS)
                    .isFeatureDisabled());

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getTraceListenerMaxReceivedLength(),
                    proxyConfig.getTraceListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-plaintext-trace-" + port);
    logger.info("listening on port: " + port + " for trace data");
  }

  @VisibleForTesting
  protected void startCustomTracingListener(
      final int port,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable WavefrontSender wfSender,
      SpanSampler sampler) {
    registerPrefixFilter(port);
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);
    WavefrontInternalReporter wfInternalReporter = null;
    if (wfSender != null) {
      wfInternalReporter =
          new WavefrontInternalReporter.Builder()
              .prefixedWith("tracing.derived")
              .withSource("custom_tracing")
              .reportMinuteDistribution()
              .build(wfSender);
      // Start the reporter
      wfInternalReporter.start(1, TimeUnit.MINUTES);
    }

    ChannelHandler channelHandler =
        new CustomTracingPortUnificationHandler(
            port,
            tokenAuthenticator,
            healthCheckManager,
            new SpanDecoder("unknown"),
            new SpanLogsDecoder(),
            preprocessors.get(port),
            handlerFactory,
            sampler,
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE)
                    .isFeatureDisabled(),
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE_SPAN_LOGS)
                    .isFeatureDisabled(),
            wfSender,
            wfInternalReporter,
            proxyConfig.getTraceDerivedCustomTagKeys(),
            proxyConfig.getCustomTracingApplicationName(),
            proxyConfig.getCustomTracingServiceName());

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getTraceListenerMaxReceivedLength(),
                    proxyConfig.getTraceListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-custom-trace-" + port);
    logger.info("listening on port: " + port + " for custom trace data");
  }

  protected void startTraceJaegerListener(
      int port,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable WavefrontSender wfSender,
      SpanSampler sampler) {
    if (tokenAuthenticator.authRequired()) {
      logger.warn("Port: " + port + " is not compatible with HTTP authentication, ignoring");
      return;
    }
    startAsManagedThread(
        port,
        () -> {
          activeListeners.inc();
          try {
            TChannel server = new TChannel.Builder("jaeger-collector").setServerPort(port).build();
            server
                .makeSubChannel("jaeger-collector", Connection.Direction.IN)
                .register(
                    "Collector::submitBatches",
                    new JaegerTChannelCollectorHandler(
                        port,
                        handlerFactory,
                        wfSender,
                        () ->
                            entityPropertiesFactoryMap
                                .get(CENTRAL_TENANT_NAME)
                                .get(ReportableEntityType.TRACE)
                                .isFeatureDisabled(),
                        () ->
                            entityPropertiesFactoryMap
                                .get(CENTRAL_TENANT_NAME)
                                .get(ReportableEntityType.TRACE_SPAN_LOGS)
                                .isFeatureDisabled(),
                        preprocessors.get(port),
                        sampler,
                        proxyConfig.getTraceJaegerApplicationName(),
                        proxyConfig.getTraceDerivedCustomTagKeys()));
            server.listen().channel().closeFuture().sync();
            server.shutdown(false);
          } catch (InterruptedException e) {
            logger.info("Listener on port " + port + " shut down.");
          } catch (Exception e) {
            logger.error("Jaeger trace collector exception", e);
          } finally {
            activeListeners.dec();
          }
        },
        "listener-jaeger-tchannel-" + port);
    logger.info("listening on port: " + port + " for trace data (Jaeger format over TChannel)");
  }

  protected void startTraceJaegerHttpListener(
      final int port,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable WavefrontSender wfSender,
      SpanSampler sampler) {
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);

    ChannelHandler channelHandler =
        new JaegerPortUnificationHandler(
            port,
            tokenAuthenticator,
            healthCheckManager,
            handlerFactory,
            wfSender,
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE)
                    .isFeatureDisabled(),
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE_SPAN_LOGS)
                    .isFeatureDisabled(),
            preprocessors.get(port),
            sampler,
            proxyConfig.getTraceJaegerApplicationName(),
            proxyConfig.getTraceDerivedCustomTagKeys());

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getTraceListenerMaxReceivedLength(),
                    proxyConfig.getTraceListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-jaeger-http-" + port);
    logger.info("listening on port: " + port + " for trace data (Jaeger format over HTTP)");
  }

  protected void startTraceJaegerGrpcListener(
      final int port,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable WavefrontSender wfSender,
      SpanSampler sampler) {
    if (tokenAuthenticator.authRequired()) {
      logger.warn("Port: " + port + " is not compatible with HTTP authentication, ignoring");
      return;
    }
    startAsManagedThread(
        port,
        () -> {
          activeListeners.inc();
          try {
            io.grpc.Server server =
                NettyServerBuilder.forPort(port)
                    .addService(
                        new JaegerGrpcCollectorHandler(
                            port,
                            handlerFactory,
                            wfSender,
                            () ->
                                entityPropertiesFactoryMap
                                    .get(CENTRAL_TENANT_NAME)
                                    .get(ReportableEntityType.TRACE)
                                    .isFeatureDisabled(),
                            () ->
                                entityPropertiesFactoryMap
                                    .get(CENTRAL_TENANT_NAME)
                                    .get(ReportableEntityType.TRACE_SPAN_LOGS)
                                    .isFeatureDisabled(),
                            preprocessors.get(port),
                            sampler,
                            proxyConfig.getTraceJaegerApplicationName(),
                            proxyConfig.getTraceDerivedCustomTagKeys()))
                    .build();
            server.start();
          } catch (Exception e) {
            logger.error("Jaeger gRPC trace collector exception", e);
          } finally {
            activeListeners.dec();
          }
        },
        "listener-jaeger-grpc-" + port);
    logger.info(
        "listening on port: " + port + " for trace data " + "(Jaeger Protobuf format over gRPC)");
  }

  protected void startOtlpGrpcListener(
      final int port,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable WavefrontSender wfSender,
      SpanSampler sampler) {
    registerPrefixFilter(port);
    startAsManagedThread(
        port,
        () -> {
          activeListeners.inc();
          try {
            OtlpGrpcTraceHandler traceHandler =
                new OtlpGrpcTraceHandler(
                    port,
                    handlerFactory,
                    wfSender,
                    preprocessors.get(port),
                    sampler,
                    () ->
                        entityPropertiesFactoryMap
                            .get(CENTRAL_TENANT_NAME)
                            .get(ReportableEntityType.TRACE)
                            .isFeatureDisabled(),
                    () ->
                        entityPropertiesFactoryMap
                            .get(CENTRAL_TENANT_NAME)
                            .get(ReportableEntityType.TRACE_SPAN_LOGS)
                            .isFeatureDisabled(),
                    proxyConfig.getHostname(),
                    proxyConfig.getTraceDerivedCustomTagKeys());
            OtlpGrpcMetricsHandler metricsHandler =
                new OtlpGrpcMetricsHandler(
                    port,
                    handlerFactory,
                    preprocessors.get(port),
                    proxyConfig.getHostname(),
                    proxyConfig.isOtlpResourceAttrsOnMetricsIncluded(),
                    proxyConfig.isOtlpAppTagsOnMetricsIncluded());
            io.grpc.Server server =
                NettyServerBuilder.forPort(port)
                    .addService(traceHandler)
                    .addService(metricsHandler)
                    .build();
            server.start();
          } catch (Exception e) {
            logger.error("OTLP gRPC collector exception", e);
          } finally {
            activeListeners.dec();
          }
        },
        "listener-otlp-grpc-" + port);
    logger.info("listening on port: " + port + " for OTLP data over gRPC");
  }

  protected void startOtlpHttpListener(
      int port,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable WavefrontSender wfSender,
      SpanSampler sampler) {
    registerPrefixFilter(port);
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);

    ChannelHandler channelHandler =
        new OtlpHttpHandler(
            handlerFactory,
            tokenAuthenticator,
            healthCheckManager,
            port,
            wfSender,
            preprocessors.get(port),
            sampler,
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE)
                    .isFeatureDisabled(),
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE_SPAN_LOGS)
                    .isFeatureDisabled(),
            proxyConfig.getHostname(),
            proxyConfig.getTraceDerivedCustomTagKeys(),
            proxyConfig.isOtlpResourceAttrsOnMetricsIncluded(),
            proxyConfig.isOtlpAppTagsOnMetricsIncluded());

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getPushListenerMaxReceivedLength(),
                    proxyConfig.getPushListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-otlp-http-" + port);
    logger.info("listening on port: " + port + " for OTLP data over HTTP");
  }

  protected void startTraceZipkinListener(
      int port,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable WavefrontSender wfSender,
      SpanSampler sampler) {
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);
    ChannelHandler channelHandler =
        new ZipkinPortUnificationHandler(
            port,
            healthCheckManager,
            handlerFactory,
            wfSender,
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE)
                    .isFeatureDisabled(),
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE_SPAN_LOGS)
                    .isFeatureDisabled(),
            preprocessors.get(port),
            sampler,
            proxyConfig.getTraceZipkinApplicationName(),
            proxyConfig.getTraceDerivedCustomTagKeys());
    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getTraceListenerMaxReceivedLength(),
                    proxyConfig.getTraceListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-zipkin-trace-" + port);
    logger.info("listening on port: " + port + " for trace data (Zipkin format)");
  }

  @VisibleForTesting
  protected void startGraphiteListener(
      int port,
      ReportableEntityHandlerFactory handlerFactory,
      SharedGraphiteHostAnnotator hostAnnotator,
      SpanSampler sampler) {
    registerPrefixFilter(port);
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);

    WavefrontPortUnificationHandler wavefrontPortUnificationHandler =
        new WavefrontPortUnificationHandler(
            port,
            tokenAuthenticator,
            healthCheckManager,
            decoderSupplier.get(),
            handlerFactory,
            hostAnnotator,
            preprocessors.get(port),
            // histogram/trace/span log feature flags consult to the central cluster
            // configuration
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.HISTOGRAM)
                    .isFeatureDisabled(),
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE)
                    .isFeatureDisabled(),
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE_SPAN_LOGS)
                    .isFeatureDisabled(),
            sampler,
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.LOGS)
                    .isFeatureDisabled(),
            proxyConfig.receivedLogServerDetails(),
            proxyConfig.enableHyperlogsConvergedCsp());

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    wavefrontPortUnificationHandler,
                    port,
                    proxyConfig.getPushListenerMaxReceivedLength(),
                    proxyConfig.getPushListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-graphite-" + port);
  }

  @VisibleForTesting
  protected void startDeltaCounterListener(
      int port, SharedGraphiteHostAnnotator hostAnnotator, SpanSampler sampler) {
    registerPrefixFilter(port);
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);

    if (this.deltaCounterHandlerFactory == null) {
      this.deltaCounterHandlerFactory =
          new ReportableEntityHandlerFactory() {
            private final Map<String, ReportableEntityHandler<?>> handlers =
                new ConcurrentHashMap<>();

            @Override
            public <T> ReportableEntityHandler<T> getHandler(String handler, QueueInfo queue) {
              return (ReportableEntityHandler<T>)
                  handlers.computeIfAbsent(
                      handler,
                      k ->
                          new DeltaCounterAccumulationHandlerImpl(
                              handler,
                              queue,
                              validationConfiguration,
                              proxyConfig.getDeltaCountersAggregationIntervalSeconds(),
                              blockedPointsLogger));
            }

            @Override
            public void shutdown(int handle) {
              if (handlers.containsKey(String.valueOf(handle))) {
                handlers.values().forEach(ReportableEntityHandler::shutdown);
              }
            }
          };
    }
    shutdownTasks.add(() -> deltaCounterHandlerFactory.shutdown(port));

    WavefrontPortUnificationHandler wavefrontPortUnificationHandler =
        new WavefrontPortUnificationHandler(
            port,
            tokenAuthenticator,
            healthCheckManager,
            decoderSupplier.get(),
            deltaCounterHandlerFactory,
            hostAnnotator,
            preprocessors.get(port),
            () -> false,
            () -> false,
            () -> false,
            sampler,
            () -> false,
            proxyConfig.receivedLogServerDetails(),
            proxyConfig.enableHyperlogsConvergedCsp());

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    wavefrontPortUnificationHandler,
                    port,
                    proxyConfig.getPushListenerMaxReceivedLength(),
                    proxyConfig.getPushListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-deltaCounter-" + port);
  }

  @VisibleForTesting
  protected void startRelayListener(
      int port,
      ReportableEntityHandlerFactory handlerFactory,
      SharedGraphiteHostAnnotator hostAnnotator) {
    registerPrefixFilter(port);
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);

    ReportableEntityHandlerFactory handlerFactoryDelegate =
        proxyConfig.isPushRelayHistogramAggregator()
            ? new DelegatingReportableEntityHandlerFactoryImpl(handlerFactory) {
              @Override
              public <T> ReportableEntityHandler<T> getHandler(String handler, QueueInfo queue) {
                if (queue.getEntityType() == ReportableEntityType.HISTOGRAM) {
                  ChronicleMap<HistogramKey, AgentDigest> accumulator =
                      ChronicleMap.of(HistogramKey.class, AgentDigest.class)
                          .keyMarshaller(HistogramKeyMarshaller.get())
                          .valueMarshaller(AgentDigestMarshaller.get())
                          .entries(proxyConfig.getPushRelayHistogramAggregatorAccumulatorSize())
                          .averageKeySize(proxyConfig.getHistogramDistAvgKeyBytes())
                          .averageValueSize(proxyConfig.getHistogramDistAvgDigestBytes())
                          .maxBloatFactor(1000)
                          .create();
                  AgentDigestFactory agentDigestFactory =
                      new AgentDigestFactory(
                          () ->
                              (short)
                                  Math.min(
                                      proxyConfig.getPushRelayHistogramAggregatorCompression(),
                                      entityPropertiesFactoryMap
                                          .get(CENTRAL_TENANT_NAME)
                                          .getGlobalProperties()
                                          .getHistogramStorageAccuracy()),
                          TimeUnit.SECONDS.toMillis(
                              proxyConfig.getPushRelayHistogramAggregatorFlushSecs()),
                          proxyConfig.getTimeProvider());
                  AccumulationCache cachedAccumulator =
                      new AccumulationCache(
                          accumulator,
                          agentDigestFactory,
                          0,
                          "histogram.accumulator.distributionRelay",
                          null);
                  // noinspection unchecked
                  return (ReportableEntityHandler<T>)
                      new HistogramAccumulationHandlerImpl(
                          handler,
                          queue,
                          cachedAccumulator,
                          null,
                          validationConfiguration,
                          blockedHistogramsLogger);
                }
                return delegate.getHandler(handler, queue);
              }
            }
            : handlerFactory;

    Map<ReportableEntityType, ReportableEntityDecoder<?, ?>> filteredDecoders =
        decoderSupplier.get().entrySet().stream()
            .filter(x -> !x.getKey().equals(ReportableEntityType.SOURCE_TAG))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    ChannelHandler channelHandler =
        new RelayPortUnificationHandler(
            port,
            tokenAuthenticator,
            healthCheckManager,
            filteredDecoders,
            handlerFactoryDelegate,
            preprocessors.get(port),
            hostAnnotator,
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.HISTOGRAM)
                    .isFeatureDisabled(),
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE)
                    .isFeatureDisabled(),
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.TRACE_SPAN_LOGS)
                    .isFeatureDisabled(),
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.LOGS)
                    .isFeatureDisabled(),
            apiContainer,
            proxyConfig);
    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getPushListenerMaxReceivedLength(),
                    proxyConfig.getPushListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-relay-" + port);
  }

  protected void startLogsIngestionListener(int port, LogsIngester logsIngester) {
    if (tokenAuthenticator.authRequired()) {
      logger.warn("Filebeat log ingestion is not compatible with HTTP authentication, ignoring");
      return;
    }
    final Server filebeatServer =
        new Server(
            "0.0.0.0",
            port,
            proxyConfig.getListenerIdleConnectionTimeout(),
            Runtime.getRuntime().availableProcessors());
    filebeatServer.setMessageListener(
        new FilebeatIngester(logsIngester, System::currentTimeMillis));
    startAsManagedThread(
        port,
        () -> {
          try {
            activeListeners.inc();
            filebeatServer.listen();
          } catch (InterruptedException e) {
            logger.info("Filebeat server on port " + port + " shut down");
          } catch (Exception e) {
            // ChannelFuture throws undeclared checked exceptions, so we need to
            // handle it
            // noinspection ConstantConditions
            if (e instanceof BindException) {
              bindErrors.inc();
              logger.error("Unable to start listener - port " + port + " is already in use!");
            } else {
              logger.error("Filebeat exception", e);
            }
          } finally {
            activeListeners.dec();
          }
        },
        "listener-logs-filebeat-" + port);
    logger.info("listening on port: " + port + " for Filebeat logs");
  }

  @VisibleForTesting
  protected void startRawLogsIngestionListener(int port, LogsIngester logsIngester) {
    if (proxyConfig.isHttpHealthCheckAllPorts()) healthCheckManager.enableHealthcheck(port);
    ChannelHandler channelHandler =
        new RawLogsIngesterPortUnificationHandler(
            port,
            logsIngester,
            hostnameResolver,
            tokenAuthenticator,
            healthCheckManager,
            preprocessors.get(port));

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getRawLogsMaxReceivedLength(),
                    proxyConfig.getRawLogsHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-logs-raw-" + port);
    logger.info("listening on port: " + port + " for raw logs");
  }

  @VisibleForTesting
  protected void startAdminListener(int port) {
    ChannelHandler channelHandler =
        new AdminPortUnificationHandler(
            tokenAuthenticator,
            healthCheckManager,
            port,
            proxyConfig.getAdminApiRemoteIpAllowRegex());

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getPushListenerMaxReceivedLength(),
                    proxyConfig.getPushListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-http-admin-" + port);
    logger.info("Admin port: " + port);
  }

  @VisibleForTesting
  protected void startHealthCheckListener(int port) {
    healthCheckManager.enableHealthcheck(port);
    ChannelHandler channelHandler = new HttpHealthCheckEndpointHandler(healthCheckManager, port);

    startAsManagedThread(
        port,
        new TcpIngester(
                createInitializer(
                    channelHandler,
                    port,
                    proxyConfig.getPushListenerMaxReceivedLength(),
                    proxyConfig.getPushListenerHttpBufferSize(),
                    proxyConfig.getListenerIdleConnectionTimeout(),
                    getSslContext(port),
                    getCorsConfig(port)),
                port)
            .withChildChannelOptions(childChannelOptions),
        "listener-http-healthcheck-" + port);
    logger.info("Health check port enabled: " + port);
  }

  protected void startHistogramListeners(
      List<Integer> ports,
      ReportableEntityHandler<ReportPoint> pointHandler,
      SharedGraphiteHostAnnotator hostAnnotator,
      @Nullable Granularity granularity,
      int flushSecs,
      boolean memoryCacheEnabled,
      File baseDirectory,
      Long accumulatorSize,
      int avgKeyBytes,
      int avgDigestBytes,
      short compression,
      boolean persist,
      SpanSampler sampler)
      throws Exception {
    if (ports.size() == 0) return;
    String listenerBinType = HistogramUtils.granularityToString(granularity);
    // Accumulator
    if (persist) {
      // Check directory
      checkArgument(
          baseDirectory.isDirectory(), baseDirectory.getAbsolutePath() + " must be a directory!");
      checkArgument(
          baseDirectory.canWrite(), baseDirectory.getAbsolutePath() + " must be write-able!");
    }

    MapLoader<HistogramKey, AgentDigest, HistogramKeyMarshaller, AgentDigestMarshaller> mapLoader =
        new MapLoader<>(
            HistogramKey.class,
            AgentDigest.class,
            accumulatorSize,
            avgKeyBytes,
            avgDigestBytes,
            HistogramKeyMarshaller.get(),
            AgentDigestMarshaller.get(),
            persist);
    File accumulationFile = new File(baseDirectory, "accumulator." + listenerBinType);
    ChronicleMap<HistogramKey, AgentDigest> accumulator = mapLoader.get(accumulationFile);

    histogramExecutor.scheduleWithFixedDelay(
        () -> {
          // warn if accumulator is more than 1.5x the original size,
          // as ChronicleMap starts losing efficiency
          if (accumulator.size() > accumulatorSize * 5) {
            logger.error(
                "Histogram "
                    + listenerBinType
                    + " accumulator size ("
                    + accumulator.size()
                    + ") is more than 5x higher than currently configured size ("
                    + accumulatorSize
                    + "), which may cause severe performance degradation issues "
                    + "or data loss! If the data volume is expected to stay at this level, we strongly "
                    + "recommend increasing the value for accumulator size in wavefront.conf and "
                    + "restarting the proxy.");
          } else if (accumulator.size() > accumulatorSize * 2) {
            logger.warn(
                "Histogram "
                    + listenerBinType
                    + " accumulator size ("
                    + accumulator.size()
                    + ") is more than 2x higher than currently configured size ("
                    + accumulatorSize
                    + "), which may cause performance issues. If the data volume is "
                    + "expected to stay at this level, we strongly recommend increasing the value "
                    + "for accumulator size in wavefront.conf and restarting the proxy.");
          }
        },
        10,
        10,
        TimeUnit.SECONDS);

    AgentDigestFactory agentDigestFactory =
        new AgentDigestFactory(
            () ->
                (short)
                    Math.min(
                        compression,
                        entityPropertiesFactoryMap
                            .get(CENTRAL_TENANT_NAME)
                            .getGlobalProperties()
                            .getHistogramStorageAccuracy()),
            TimeUnit.SECONDS.toMillis(flushSecs),
            proxyConfig.getTimeProvider());
    Accumulator cachedAccumulator =
        new AccumulationCache(
            accumulator,
            agentDigestFactory,
            (memoryCacheEnabled ? accumulatorSize : 0),
            "histogram.accumulator." + HistogramUtils.granularityToString(granularity),
            null);

    // Schedule write-backs
    histogramExecutor.scheduleWithFixedDelay(
        cachedAccumulator::flush,
        proxyConfig.getHistogramAccumulatorResolveInterval(),
        proxyConfig.getHistogramAccumulatorResolveInterval(),
        TimeUnit.MILLISECONDS);
    histogramFlushRunnables.add(cachedAccumulator::flush);

    PointHandlerDispatcher dispatcher =
        new PointHandlerDispatcher(
            cachedAccumulator,
            pointHandler,
            proxyConfig.getTimeProvider(),
            () ->
                entityPropertiesFactoryMap
                    .get(CENTRAL_TENANT_NAME)
                    .get(ReportableEntityType.HISTOGRAM)
                    .isFeatureDisabled(),
            proxyConfig.getHistogramAccumulatorFlushMaxBatchSize() < 0
                ? null
                : proxyConfig.getHistogramAccumulatorFlushMaxBatchSize(),
            granularity);

    histogramExecutor.scheduleWithFixedDelay(
        dispatcher,
        proxyConfig.getHistogramAccumulatorFlushInterval(),
        proxyConfig.getHistogramAccumulatorFlushInterval(),
        TimeUnit.MILLISECONDS);
    histogramFlushRunnables.add(dispatcher);

    // gracefully shutdown persisted accumulator (ChronicleMap) on proxy exit
    shutdownTasks.add(
        () -> {
          try {
            logger.info("Flushing in-flight histogram accumulator digests: " + listenerBinType);
            cachedAccumulator.flush();
            logger.info("Shutting down histogram accumulator cache: " + listenerBinType);
            accumulator.close();
          } catch (Throwable t) {
            logger.error(
                "Error flushing " + listenerBinType + " accumulator, possibly unclean shutdown: ",
                t);
          }
        });

    ReportableEntityHandlerFactory histogramHandlerFactory =
        new ReportableEntityHandlerFactory() {
          private final Map<QueueInfo, ReportableEntityHandler<?>> handlers =
              new ConcurrentHashMap<>();

          @SuppressWarnings("unchecked")
          @Override
          public <T> ReportableEntityHandler<T> getHandler(String handler, QueueInfo queue) {
            return (ReportableEntityHandler<T>)
                handlers.computeIfAbsent(
                    queue,
                    k ->
                        new HistogramAccumulationHandlerImpl(
                            handler,
                            queue,
                            cachedAccumulator,
                            granularity,
                            validationConfiguration,
                            blockedHistogramsLogger));
          }

          @Override
          public void shutdown(int handle) {
            handlers.values().forEach(ReportableEntityHandler::shutdown);
          }
        };

    ports.forEach(
        port -> {
          registerPrefixFilter(port);
          if (proxyConfig.isHttpHealthCheckAllPorts()) {
            healthCheckManager.enableHealthcheck(port);
          }
          WavefrontPortUnificationHandler wavefrontPortUnificationHandler =
              new WavefrontPortUnificationHandler(
                  port,
                  tokenAuthenticator,
                  healthCheckManager,
                  decoderSupplier.get(),
                  histogramHandlerFactory,
                  hostAnnotator,
                  preprocessors.get(port),
                  () ->
                      entityPropertiesFactoryMap
                          .get(CENTRAL_TENANT_NAME)
                          .get(ReportableEntityType.HISTOGRAM)
                          .isFeatureDisabled(),
                  () ->
                      entityPropertiesFactoryMap
                          .get(CENTRAL_TENANT_NAME)
                          .get(ReportableEntityType.TRACE)
                          .isFeatureDisabled(),
                  () ->
                      entityPropertiesFactoryMap
                          .get(CENTRAL_TENANT_NAME)
                          .get(ReportableEntityType.TRACE_SPAN_LOGS)
                          .isFeatureDisabled(),
                  sampler,
                  () ->
                      entityPropertiesFactoryMap
                          .get(CENTRAL_TENANT_NAME)
                          .get(ReportableEntityType.LOGS)
                          .isFeatureDisabled(),
                  proxyConfig.receivedLogServerDetails(),
                  proxyConfig.enableHyperlogsConvergedCsp());

          startAsManagedThread(
              port,
              new TcpIngester(
                      createInitializer(
                          wavefrontPortUnificationHandler,
                          port,
                          proxyConfig.getHistogramMaxReceivedLength(),
                          proxyConfig.getHistogramHttpBufferSize(),
                          proxyConfig.getListenerIdleConnectionTimeout(),
                          getSslContext(port),
                          getCorsConfig(port)),
                      port)
                  .withChildChannelOptions(childChannelOptions),
              "listener-histogram-" + port);
          logger.info(
              "listening on port: "
                  + port
                  + " for histogram samples, accumulating to the "
                  + listenerBinType);
        });
  }

  private void registerPrefixFilter(int port) {
    if (proxyConfig.getPrefix() != null && !proxyConfig.getPrefix().isEmpty()) {
      preprocessors
          .getSystemPreprocessor(port)
          .forReportPoint()
          .addTransformer(new ReportPointAddPrefixTransformer(proxyConfig.getPrefix()));
    }
  }

  /**
   * Push agent configuration during check-in by the collector.
   *
   * @param tenantName The tenant name to which config corresponding
   * @param config The configuration to process.
   */
  @Override
  protected void processConfiguration(String tenantName, AgentConfiguration config) {
    try {
      Long pointsPerBatch = config.getPointsPerBatch();
      EntityPropertiesFactory tenantSpecificEntityProps =
          entityPropertiesFactoryMap.get(tenantName);
      if (BooleanUtils.isTrue(config.getCollectorSetsPointsPerBatch())) {
        if (pointsPerBatch != null) {
          // if the collector is in charge and it provided a setting, use it
          tenantSpecificEntityProps
              .get(ReportableEntityType.POINT)
              .setDataPerBatch(pointsPerBatch.intValue());
          logger.info("Proxy push batch set to (remotely) " + pointsPerBatch);
        } // otherwise don't change the setting
      } else {
        // restore the original setting
        tenantSpecificEntityProps.get(ReportableEntityType.POINT).setDataPerBatch(null);
        logger.info(
            "Proxy push batch set to (locally) "
                + tenantSpecificEntityProps.get(ReportableEntityType.POINT).getDataPerBatch());
      }
      if (config.getHistogramStorageAccuracy() != null) {
        tenantSpecificEntityProps
            .getGlobalProperties()
            .setHistogramStorageAccuracy(config.getHistogramStorageAccuracy().shortValue());
      }
      if (!proxyConfig.isBackendSpanHeadSamplingPercentIgnored()) {
        double previousSamplingRate =
            tenantSpecificEntityProps.getGlobalProperties().getTraceSamplingRate();
        tenantSpecificEntityProps
            .getGlobalProperties()
            .setTraceSamplingRate(config.getSpanSamplingRate());
        rateSampler.setSamplingRate(
            tenantSpecificEntityProps.getGlobalProperties().getTraceSamplingRate());
        if (previousSamplingRate
            != tenantSpecificEntityProps.getGlobalProperties().getTraceSamplingRate()) {
          logger.info(
              "Proxy trace span sampling rate set to "
                  + tenantSpecificEntityProps.getGlobalProperties().getTraceSamplingRate());
        }
      }
      tenantSpecificEntityProps
          .getGlobalProperties()
          .setDropSpansDelayedMinutes(config.getDropSpansDelayedMinutes());
      tenantSpecificEntityProps
          .getGlobalProperties()
          .setActiveSpanSamplingPolicies(config.getActiveSpanSamplingPolicies());

      updateRateLimiter(
          tenantName,
          ReportableEntityType.POINT,
          config.getCollectorSetsRateLimit(),
          config.getCollectorRateLimit(),
          config.getGlobalCollectorRateLimit());
      updateRateLimiter(
          tenantName,
          ReportableEntityType.HISTOGRAM,
          config.getCollectorSetsRateLimit(),
          config.getHistogramRateLimit(),
          config.getGlobalHistogramRateLimit());
      updateRateLimiter(
          tenantName,
          ReportableEntityType.SOURCE_TAG,
          config.getCollectorSetsRateLimit(),
          config.getSourceTagsRateLimit(),
          config.getGlobalSourceTagRateLimit());
      updateRateLimiter(
          tenantName,
          ReportableEntityType.TRACE,
          config.getCollectorSetsRateLimit(),
          config.getSpanRateLimit(),
          config.getGlobalSpanRateLimit());
      updateRateLimiter(
          tenantName,
          ReportableEntityType.TRACE_SPAN_LOGS,
          config.getCollectorSetsRateLimit(),
          config.getSpanLogsRateLimit(),
          config.getGlobalSpanLogsRateLimit());
      updateRateLimiter(
          tenantName,
          ReportableEntityType.EVENT,
          config.getCollectorSetsRateLimit(),
          config.getEventsRateLimit(),
          config.getGlobalEventRateLimit());
      updateRateLimiter(
          tenantName,
          ReportableEntityType.LOGS,
          config.getCollectorSetsRateLimit(),
          config.getLogsRateLimit(),
          config.getGlobalLogsRateLimit());

      tenantSpecificEntityProps
          .get(ReportableEntityType.HISTOGRAM)
          .setFeatureDisabled(BooleanUtils.isTrue(config.getHistogramDisabled()));
      tenantSpecificEntityProps
          .get(ReportableEntityType.TRACE)
          .setFeatureDisabled(BooleanUtils.isTrue(config.getTraceDisabled()));
      tenantSpecificEntityProps
          .get(ReportableEntityType.TRACE_SPAN_LOGS)
          .setFeatureDisabled(BooleanUtils.isTrue(config.getSpanLogsDisabled()));
      tenantSpecificEntityProps
          .get(ReportableEntityType.LOGS)
          .setFeatureDisabled(BooleanUtils.isTrue(config.getLogsDisabled()));
      validationConfiguration.updateFrom(config.getValidationConfiguration());
    } catch (RuntimeException e) {
      // cannot throw or else configuration update thread would die, so just log it.
      logger.warn("Error during configuration update", e);
    }
    try {
      super.processConfiguration(tenantName, config);
    } catch (RuntimeException e) {
      // cannot throw or else configuration update thread would die. it's ok to ignore
      // these.
    }
  }

  private void updateRateLimiter(
      String tenantName,
      ReportableEntityType entityType,
      @Nullable Boolean collectorSetsRateLimit,
      @Nullable Number collectorRateLimit,
      @Nullable Number globalRateLimit) {
    EntityProperties entityProperties = entityPropertiesFactoryMap.get(tenantName).get(entityType);
    EntityRateLimiter rateLimiter = entityProperties.getRateLimiter();
    if (rateLimiter != null) {
      if (BooleanUtils.isTrue(collectorSetsRateLimit)) {
        if (collectorRateLimit != null
            && rateLimiter.getRate() != collectorRateLimit.doubleValue()) {
          rateLimiter.setRate(collectorRateLimit.doubleValue());
          entityProperties.setDataPerBatch(
              Math.min(collectorRateLimit.intValue(), entityProperties.getDataPerBatch()));
          logger.warn(
              "["
                  + tenantName
                  + "]: "
                  + entityType.toCapitalizedString()
                  + " rate limit set to "
                  + collectorRateLimit
                  + entityType.getRateUnit()
                  + " remotely");
        }
      } else {
        double rateLimit =
            Math.min(
                entityProperties.getRateLimit(),
                ObjectUtils.firstNonNull(globalRateLimit, NO_RATE_LIMIT).intValue());
        if (rateLimiter.getRate() != rateLimit) {
          rateLimiter.setRate(rateLimit);
          if (entityProperties.getDataPerBatchOriginal() > rateLimit) {
            entityProperties.setDataPerBatch((int) rateLimit);
          } else {
            entityProperties.setDataPerBatch(null);
          }
          if (rateLimit >= NO_RATE_LIMIT) {
            logger.warn(
                entityType.toCapitalizedString()
                    + " rate limit is no longer "
                    + "enforced by remote");
          } else {
            if (proxyCheckinScheduler != null
                && proxyCheckinScheduler.getSuccessfulCheckinCount() > 1) {
              // this will skip printing this message upon init
              logger.warn(
                  entityType.toCapitalizedString()
                      + " rate limit restored to "
                      + rateLimit
                      + entityType.getRateUnit());
            }
          }
        }
      }
    }
  }

  protected TokenAuthenticator configureTokenAuthenticator() {
    HttpClient httpClient =
        HttpClientBuilder.create()
            .useSystemProperties()
            .setUserAgent(proxyConfig.getHttpUserAgent())
            .setMaxConnPerRoute(10)
            .setMaxConnTotal(10)
            .setConnectionTimeToLive(1, TimeUnit.MINUTES)
            .setRetryHandler(
                new DefaultHttpRequestRetryHandler(proxyConfig.getHttpAutoRetries(), true))
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setContentCompressionEnabled(true)
                    .setRedirectsEnabled(true)
                    .setConnectTimeout(proxyConfig.getHttpConnectTimeout())
                    .setConnectionRequestTimeout(proxyConfig.getHttpConnectTimeout())
                    .setSocketTimeout(proxyConfig.getHttpRequestTimeout())
                    .build())
            .build();
    return TokenAuthenticatorBuilder.create()
        .setTokenValidationMethod(proxyConfig.getAuthMethod())
        .setHttpClient(httpClient)
        .setTokenIntrospectionServiceUrl(proxyConfig.getAuthTokenIntrospectionServiceUrl())
        .setTokenIntrospectionAuthorizationHeader(
            proxyConfig.getAuthTokenIntrospectionAuthorizationHeader())
        .setAuthResponseRefreshInterval(proxyConfig.getAuthResponseRefreshInterval())
        .setAuthResponseMaxTtl(proxyConfig.getAuthResponseMaxTtl())
        .setStaticToken(proxyConfig.getAuthStaticToken())
        .build();
  }

  protected void startAsManagedThread(int port, Runnable target, @Nullable String threadName) {
    Thread thread = new Thread(target);
    if (threadName != null) {
      thread.setName(threadName);
    }
    listeners.put(port, thread);
    thread.start();
  }

  @Override
  public void stopListeners() {
    listeners.values().forEach(Thread::interrupt);
    listeners
        .values()
        .forEach(
            thread -> {
              try {
                thread.join(TimeUnit.SECONDS.toMillis(10));
              } catch (InterruptedException e) {
                // ignore
              }
            });
  }
}
