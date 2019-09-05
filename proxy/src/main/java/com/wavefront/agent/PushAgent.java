package com.wavefront.agent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.tdunning.math.stats.AgentDigest;
import com.tdunning.math.stats.AgentDigest.AgentDigestMarshaller;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.channels.Connection;
import com.wavefront.agent.channel.CachingHostnameLookupResolver;
import com.wavefront.agent.channel.ConnectionTrackingHandler;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.channel.HealthCheckManagerImpl;
import com.wavefront.agent.channel.IdleStateEventHandler;
import com.wavefront.agent.channel.PlainTextOrHttpFrameDecoder;
import com.wavefront.agent.channel.SharedGraphiteHostAnnotator;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.formatter.GraphiteFormatter;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.HistogramAccumulationHandlerImpl;
import com.wavefront.agent.handlers.InternalProxyWavefrontClient;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactoryImpl;
import com.wavefront.agent.handlers.SenderTaskFactory;
import com.wavefront.agent.handlers.SenderTaskFactoryImpl;
import com.wavefront.agent.histogram.MapLoader;
import com.wavefront.agent.histogram.PointHandlerDispatcher;
import com.wavefront.agent.histogram.Utils;
import com.wavefront.agent.histogram.Utils.HistogramKey;
import com.wavefront.agent.histogram.Utils.HistogramKeyMarshaller;
import com.wavefront.agent.histogram.accumulator.AccumulationCache;
import com.wavefront.agent.histogram.accumulator.Accumulator;
import com.wavefront.agent.listeners.AdminPortUnificationHandler;
import com.wavefront.agent.listeners.ChannelByteArrayHandler;
import com.wavefront.agent.listeners.DataDogPortUnificationHandler;
import com.wavefront.agent.listeners.HttpHealthCheckEndpointHandler;
import com.wavefront.agent.listeners.JsonMetricsPortUnificationHandler;
import com.wavefront.agent.listeners.OpenTSDBPortUnificationHandler;
import com.wavefront.agent.listeners.RawLogsIngesterPortUnificationHandler;
import com.wavefront.agent.listeners.RelayPortUnificationHandler;
import com.wavefront.agent.listeners.WavefrontPortUnificationHandler;
import com.wavefront.agent.listeners.WriteHttpJsonPortUnificationHandler;
import com.wavefront.agent.listeners.tracing.JaegerThriftCollectorHandler;
import com.wavefront.agent.listeners.tracing.TracePortUnificationHandler;
import com.wavefront.agent.listeners.tracing.ZipkinPortUnificationHandler;
import com.wavefront.agent.logsharvesting.FilebeatIngester;
import com.wavefront.agent.logsharvesting.LogsIngester;
import com.wavefront.agent.preprocessor.ReportPointAddPrefixTransformer;
import com.wavefront.agent.preprocessor.ReportPointTimestampInRangeFilter;
import com.wavefront.agent.sampler.SpanSamplerUtils;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.GraphiteDecoder;
import com.wavefront.ingester.HistogramDecoder;
import com.wavefront.ingester.OpenTSDBDecoder;
import com.wavefront.ingester.PickleProtocolDecoder;
import com.wavefront.ingester.ReportPointDecoderWrapper;
import com.wavefront.ingester.ReportSourceTagDecoder;
import com.wavefront.ingester.ReportableEntityDecoder;
import com.wavefront.ingester.SpanDecoder;
import com.wavefront.ingester.SpanLogsDecoder;
import com.wavefront.ingester.TcpIngester;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.entities.tracing.sampling.CompositeSampler;
import com.wavefront.sdk.entities.tracing.sampling.Sampler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import net.openhft.chronicle.map.ChronicleMap;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.logstash.beats.Server;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.timeout.IdleStateHandler;

import wavefront.report.ReportPoint;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.RecyclableRateLimiter.UNLIMITED;
import static com.wavefront.agent.Utils.lazySupplier;

/**
 * Push-only Agent.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class PushAgent extends AbstractAgent {

  protected final List<Thread> managedThreads = new ArrayList<>();
  protected final IdentityHashMap<ChannelOption<?>, Object> childChannelOptions =
      new IdentityHashMap<>();
  protected ScheduledExecutorService histogramExecutor;
  protected ScheduledExecutorService histogramFlushExecutor;
  protected final Counter bindErrors = Metrics.newCounter(
      ExpectedAgentMetric.LISTENERS_BIND_ERRORS.metricName);
  private volatile ReportableEntityDecoder<String, ReportPoint> wavefrontDecoder;
  protected SharedGraphiteHostAnnotator remoteHostAnnotator;
  protected Function<InetAddress, String> hostnameResolver;
  protected SenderTaskFactory senderTaskFactory;
  protected ReportableEntityHandlerFactory handlerFactory;
  protected HealthCheckManager healthCheckManager;
  protected Supplier<Map<ReportableEntityType, ReportableEntityDecoder>> decoderSupplier =
          lazySupplier(() -> ImmutableMap.of(
                  ReportableEntityType.POINT, new ReportPointDecoderWrapper(new GraphiteDecoder("unknown",
                          customSourceTags)),
                  ReportableEntityType.SOURCE_TAG, new ReportSourceTagDecoder(),
                  ReportableEntityType.HISTOGRAM, new ReportPointDecoderWrapper(
                          new HistogramDecoder("unknown")),
                  ReportableEntityType.TRACE, new SpanDecoder("unknown"),
                  ReportableEntityType.TRACE_SPAN_LOGS, new SpanLogsDecoder()));

  protected final Map<ReportableEntityType, ReportableEntityDecoder> DECODERS = ImmutableMap.of(
          ReportableEntityType.POINT, getDecoderInstance(),
          ReportableEntityType.SOURCE_TAG, new ReportSourceTagDecoder(),
          ReportableEntityType.HISTOGRAM, new ReportPointDecoderWrapper(
                  new HistogramDecoder("unknown")));

  public static void main(String[] args) throws IOException {
    // Start the ssh daemon
    new PushAgent().start(args);
  }

  public PushAgent() {
    super(false, true);
  }

  @Deprecated
  protected PushAgent(boolean reportAsPushAgent) {
    super(false, reportAsPushAgent);
  }

  @VisibleForTesting
  protected ReportableEntityDecoder<String, ReportPoint> getDecoderInstance() {
    synchronized(PushAgent.class) {
      if (wavefrontDecoder == null) {
        wavefrontDecoder = new ReportPointDecoderWrapper(new GraphiteDecoder("unknown",
                customSourceTags));
      }
      return wavefrontDecoder;
    }
  }

  @Override
  protected void setupMemoryGuard(double threshold) {
    new ProxyMemoryGuard(senderTaskFactory::drainBuffersToQueue, threshold);
  }

  @Override
  protected void startListeners() {
    if (soLingerTime >= 0) {
      childChannelOptions.put(ChannelOption.SO_LINGER, soLingerTime);
    }
    hostnameResolver = new CachingHostnameLookupResolver(disableRdnsLookup,
        ExpectedAgentMetric.RDNS_CACHE_SIZE.metricName);
    remoteHostAnnotator = new SharedGraphiteHostAnnotator(customSourceTags, hostnameResolver);
    senderTaskFactory = new SenderTaskFactoryImpl(agentAPI, agentId, pushRateLimiter,
        pushFlushInterval, pushFlushMaxPoints, pushMemoryBufferLimit);
    handlerFactory = new ReportableEntityHandlerFactoryImpl(senderTaskFactory, pushBlockedSamples,
        flushThreads, () -> validationConfiguration, reportIntervalSeconds);
    healthCheckManager = new HealthCheckManagerImpl(httpHealthCheckPath,
        httpHealthCheckResponseContentType, httpHealthCheckPassStatusCode,
        httpHealthCheckPassResponseBody, httpHealthCheckFailStatusCode,
        httpHealthCheckFailResponseBody);

    shutdownTasks.add(() -> senderTaskFactory.shutdown());
    shutdownTasks.add(() -> senderTaskFactory.drainBuffersToQueue());

    if (adminApiListenerPort > 0) {
      startAdminListener(adminApiListenerPort);
    }
    portIterator(httpHealthCheckPorts).forEachRemaining(strPort ->
        startHealthCheckListener(Integer.parseInt(strPort)));

    portIterator(pushListenerPorts).forEachRemaining(strPort -> {
      startGraphiteListener(strPort, handlerFactory, remoteHostAnnotator, false);
      logger.info("listening on port: " + strPort + " for Wavefront metrics");
    });

    portIterator(deltaCountersListenerPorts).forEachRemaining(strPort -> {
      startGraphiteListener(strPort, handlerFactory, remoteHostAnnotator, true);
      logger.info("listening on port: " + strPort + " for Wavefront delta counter metrics");
    });

    {
      // Histogram bootstrap.
      Iterator<String> histMinPorts = portIterator(histogramMinuteListenerPorts);
      Iterator<String> histHourPorts = portIterator(histogramHourListenerPorts);
      Iterator<String> histDayPorts = portIterator(histogramDayListenerPorts);
      Iterator<String> histDistPorts = portIterator(histogramDistListenerPorts);

      int activeHistogramAggregationTypes = (histDayPorts.hasNext() ? 1 : 0) +
          (histHourPorts.hasNext() ? 1 : 0) + (histMinPorts.hasNext() ? 1 : 0) +
          (histDistPorts.hasNext() ? 1 : 0);
      if (activeHistogramAggregationTypes > 0) { /*Histograms enabled*/
        histogramExecutor = Executors.newScheduledThreadPool(
            1 + activeHistogramAggregationTypes, new NamedThreadFactory("histogram-service"));
        histogramFlushExecutor = Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors() / 2,
            new NamedThreadFactory("histogram-flush"));
        managedExecutors.add(histogramExecutor);
        managedExecutors.add(histogramFlushExecutor);

        File baseDirectory = new File(histogramStateDirectory);
        if (persistAccumulator) {
          // Check directory
          checkArgument(baseDirectory.isDirectory(), baseDirectory.getAbsolutePath() +
              " must be a directory!");
          checkArgument(baseDirectory.canWrite(), baseDirectory.getAbsolutePath() +
              " must be write-able!");
        }

        // Central dispatch
        @SuppressWarnings("unchecked")
        ReportableEntityHandler<ReportPoint> pointHandler = handlerFactory.getHandler(
            HandlerKey.of(ReportableEntityType.HISTOGRAM, "histogram_ports"));

        if (histMinPorts.hasNext()) {
          startHistogramListeners(histMinPorts, pointHandler, remoteHostAnnotator,
              Utils.Granularity.MINUTE, histogramMinuteFlushSecs, histogramMinuteMemoryCache,
              baseDirectory, histogramMinuteAccumulatorSize, histogramMinuteAvgKeyBytes,
              histogramMinuteAvgDigestBytes, histogramMinuteCompression);
        }

        if (histHourPorts.hasNext()) {
          startHistogramListeners(histHourPorts, pointHandler, remoteHostAnnotator,
              Utils.Granularity.HOUR, histogramHourFlushSecs, histogramHourMemoryCache,
              baseDirectory, histogramHourAccumulatorSize, histogramHourAvgKeyBytes,
              histogramHourAvgDigestBytes, histogramHourCompression);
        }

        if (histDayPorts.hasNext()) {
          startHistogramListeners(histDayPorts, pointHandler, remoteHostAnnotator,
              Utils.Granularity.DAY, histogramDayFlushSecs, histogramDayMemoryCache,
              baseDirectory, histogramDayAccumulatorSize, histogramDayAvgKeyBytes,
              histogramDayAvgDigestBytes, histogramDayCompression);
        }

        if (histDistPorts.hasNext()) {
          startHistogramListeners(histDistPorts, pointHandler, remoteHostAnnotator,
              null, histogramDistFlushSecs, histogramDistMemoryCache,
              baseDirectory, histogramDistAccumulatorSize, histogramDistAvgKeyBytes,
              histogramDistAvgDigestBytes, histogramDistCompression);
        }
      }
    }

    if (StringUtils.isNotBlank(graphitePorts) || StringUtils.isNotBlank(picklePorts)) {
      if (tokenAuthenticator.authRequired()) {
        logger.warning("Graphite mode is not compatible with HTTP authentication, ignoring");
      } else {
        Preconditions.checkNotNull(graphiteFormat,
            "graphiteFormat must be supplied to enable graphite support");
        Preconditions.checkNotNull(graphiteDelimiters,
            "graphiteDelimiters must be supplied to enable graphite support");
        GraphiteFormatter graphiteFormatter = new GraphiteFormatter(graphiteFormat,
            graphiteDelimiters, graphiteFieldsToRemove);
        portIterator(graphitePorts).forEachRemaining(strPort -> {
          preprocessors.getSystemPreprocessor(strPort).forPointLine().
              addTransformer(0, graphiteFormatter);
          startGraphiteListener(strPort, handlerFactory, null, false);
          logger.info("listening on port: " + strPort + " for graphite metrics");
        });
        portIterator(picklePorts).forEachRemaining(strPort ->
            startPickleListener(strPort, handlerFactory, graphiteFormatter));
      }
    }
    portIterator(opentsdbPorts).forEachRemaining(strPort ->
        startOpenTsdbListener(strPort, handlerFactory));
    if (dataDogJsonPorts != null) {
      HttpClient httpClient = HttpClientBuilder.create().
          useSystemProperties().
          setUserAgent(httpUserAgent).
          setConnectionTimeToLive(1, TimeUnit.MINUTES).
          setRetryHandler(new DefaultHttpRequestRetryHandler(httpAutoRetries, true)).
          setDefaultRequestConfig(
              RequestConfig.custom().
                  setContentCompressionEnabled(true).
                  setRedirectsEnabled(true).
                  setConnectTimeout(httpConnectTimeout).
                  setConnectionRequestTimeout(httpConnectTimeout).
                  setSocketTimeout(httpRequestTimeout).build()).
          build();

      portIterator(dataDogJsonPorts).forEachRemaining(strPort ->
          startDataDogListener(strPort, handlerFactory, httpClient));
    }
    // sampler for spans
    Sampler rateSampler = SpanSamplerUtils.getRateSampler(traceSamplingRate);
    Sampler durationSampler = SpanSamplerUtils.getDurationSampler(traceSamplingDuration);
    List<Sampler> samplers = SpanSamplerUtils.fromSamplers(rateSampler, durationSampler);
    Sampler compositeSampler = new CompositeSampler(samplers);

    portIterator(traceListenerPorts).forEachRemaining(strPort ->
        startTraceListener(strPort, handlerFactory, compositeSampler));
    portIterator(traceJaegerListenerPorts).forEachRemaining(strPort ->
        startTraceJaegerListener(strPort, handlerFactory,
            new InternalProxyWavefrontClient(handlerFactory, strPort), compositeSampler));
    portIterator(pushRelayListenerPorts).forEachRemaining(strPort ->
        startRelayListener(strPort, handlerFactory, remoteHostAnnotator));
    portIterator(traceZipkinListenerPorts).forEachRemaining(strPort ->
        startTraceZipkinListener(strPort, handlerFactory,
            new InternalProxyWavefrontClient(handlerFactory, strPort), compositeSampler));
    portIterator(jsonListenerPorts).forEachRemaining(strPort ->
        startJsonListener(strPort, handlerFactory));
    portIterator(writeHttpJsonListenerPorts).forEachRemaining(strPort ->
        startWriteHttpJsonListener(strPort, handlerFactory));

    // Logs ingestion.
    if ((filebeatPort > 0 || rawLogsPort > 0) && loadLogsIngestionConfig() != null) {
      logger.info("Initializing logs ingestion");
      try {
        final LogsIngester logsIngester = new LogsIngester(handlerFactory,
            this::loadLogsIngestionConfig, prefix);
        logsIngester.start();

        if (filebeatPort > 0) {
          startLogsIngestionListener(filebeatPort, logsIngester);
        }
        if (rawLogsPort > 0) {
          startRawLogsIngestionListener(rawLogsPort, logsIngester);
        }
      } catch (ConfigurationException e) {
        logger.log(Level.SEVERE, "Cannot start logsIngestion", e);
      }
    }
  }

  protected void startJsonListener(String strPort, ReportableEntityHandlerFactory handlerFactory) {
    final int port = Integer.parseInt(strPort);
    registerTimestampFilter(strPort);
    if (httpHealthCheckAllPorts) healthCheckManager.enableHealthcheck(port);

    ChannelHandler channelHandler = new JsonMetricsPortUnificationHandler(strPort,
        tokenAuthenticator, healthCheckManager, handlerFactory, prefix, hostname,
        preprocessors.get(strPort));

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort,
        pushListenerMaxReceivedLength, pushListenerHttpBufferSize, listenerIdleConnectionTimeout),
            port).withChildChannelOptions(childChannelOptions), "listener-plaintext-json-" + port);
    logger.info("listening on port: " + strPort + " for JSON metrics data");
  }

  protected void startWriteHttpJsonListener(String strPort,
                                            ReportableEntityHandlerFactory handlerFactory) {
    final int port = Integer.parseInt(strPort);
    registerPrefixFilter(strPort);
    registerTimestampFilter(strPort);
    if (httpHealthCheckAllPorts) healthCheckManager.enableHealthcheck(port);

    ChannelHandler channelHandler = new WriteHttpJsonPortUnificationHandler(strPort,
        tokenAuthenticator, healthCheckManager, handlerFactory, hostname,
        preprocessors.get(strPort));

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort,
        pushListenerMaxReceivedLength, pushListenerHttpBufferSize, listenerIdleConnectionTimeout),
            port).withChildChannelOptions(childChannelOptions),
        "listener-plaintext-writehttpjson-" + port);
    logger.info("listening on port: " + strPort + " for write_http data");
  }

  protected void startOpenTsdbListener(final String strPort,
                                       ReportableEntityHandlerFactory handlerFactory) {
    int port = Integer.parseInt(strPort);
    registerPrefixFilter(strPort);
    registerTimestampFilter(strPort);
    if (httpHealthCheckAllPorts) healthCheckManager.enableHealthcheck(port);

    ReportableEntityDecoder<String, ReportPoint> openTSDBDecoder = new ReportPointDecoderWrapper(
        new OpenTSDBDecoder("unknown", customSourceTags));

    ChannelHandler channelHandler = new OpenTSDBPortUnificationHandler(strPort, tokenAuthenticator,
        healthCheckManager, openTSDBDecoder, handlerFactory, preprocessors.get(strPort),
        hostnameResolver);

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort,
        pushListenerMaxReceivedLength, pushListenerHttpBufferSize, listenerIdleConnectionTimeout),
            port).withChildChannelOptions(childChannelOptions),
        "listener-plaintext-opentsdb-" + port);
    logger.info("listening on port: " + strPort + " for OpenTSDB metrics");
  }

  protected void startDataDogListener(final String strPort,
                                      ReportableEntityHandlerFactory handlerFactory,
                                      HttpClient httpClient) {
    if (tokenAuthenticator.authRequired()) {
      logger.warning("Port: " + strPort +
          " (DataDog) is not compatible with HTTP authentication, ignoring");
      return;
    }
    int port = Integer.parseInt(strPort);
    registerPrefixFilter(strPort);
    registerTimestampFilter(strPort);
    if (httpHealthCheckAllPorts) healthCheckManager.enableHealthcheck(port);

    ChannelHandler channelHandler = new DataDogPortUnificationHandler(strPort, healthCheckManager,
        handlerFactory, dataDogProcessSystemMetrics, dataDogProcessServiceChecks, httpClient,
        dataDogRequestRelayTarget, preprocessors.get(strPort));

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort,
        pushListenerMaxReceivedLength, pushListenerHttpBufferSize, listenerIdleConnectionTimeout),
            port).withChildChannelOptions(childChannelOptions),
        "listener-plaintext-datadog-" + port);
    logger.info("listening on port: " + strPort + " for DataDog metrics");
  }

  protected void startPickleListener(String strPort,
                                     ReportableEntityHandlerFactory handlerFactory,
                                     GraphiteFormatter formatter) {
    if (tokenAuthenticator.authRequired()) {
      logger.warning("Port: " + strPort +
          " (pickle format) is not compatible with HTTP authentication, ignoring");
      return;
    }
    int port = Integer.parseInt(strPort);
    registerPrefixFilter(strPort);
    registerTimestampFilter(strPort);

    // Set up a custom handler
    //noinspection unchecked
    ChannelHandler channelHandler = new ChannelByteArrayHandler(
        new PickleProtocolDecoder("unknown", customSourceTags, formatter.getMetricMangler(), port),
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT, strPort)),
        preprocessors.get(strPort));

    startAsManagedThread(new TcpIngester(createInitializer(ImmutableList.of(
        () -> new LengthFieldBasedFrameDecoder(ByteOrder.BIG_ENDIAN, 1000000, 0, 4, 0, 4, false),
        () -> new ByteArrayDecoder(), () -> channelHandler), strPort,
        listenerIdleConnectionTimeout), port).withChildChannelOptions(childChannelOptions),
        "listener-binary-pickle-" + strPort);
    logger.info("listening on port: " + strPort + " for Graphite/pickle protocol metrics");
  }

  protected void startTraceListener(final String strPort,
                                    ReportableEntityHandlerFactory handlerFactory,
                                    Sampler sampler) {
    final int port = Integer.parseInt(strPort);
    registerPrefixFilter(strPort);
    registerTimestampFilter(strPort);
    if (httpHealthCheckAllPorts) healthCheckManager.enableHealthcheck(port);

    ChannelHandler channelHandler = new TracePortUnificationHandler(strPort, tokenAuthenticator,
        healthCheckManager, new SpanDecoder("unknown"), new SpanLogsDecoder(),
        preprocessors.get(strPort), handlerFactory, sampler, traceAlwaysSampleErrors,
        traceDisabled::get, spanLogsDisabled::get);

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort,
        traceListenerMaxReceivedLength, traceListenerHttpBufferSize, listenerIdleConnectionTimeout),
            port).withChildChannelOptions(childChannelOptions), "listener-plaintext-trace-" + port);
    logger.info("listening on port: " + strPort + " for trace data");
  }

  protected void startTraceJaegerListener(String strPort,
                                          ReportableEntityHandlerFactory handlerFactory,
                                          @Nullable WavefrontSender wfSender,
                                          Sampler sampler) {
    if (tokenAuthenticator.authRequired()) {
      logger.warning("Port: " + strPort + " is not compatible with HTTP authentication, ignoring");
      return;
    }
    startAsManagedThread(() -> {
      activeListeners.inc();
      try {
        TChannel server = new TChannel.Builder("jaeger-collector").
            setServerPort(Integer.valueOf(strPort)).
            build();
        server.
            makeSubChannel("jaeger-collector", Connection.Direction.IN).
            register("Collector::submitBatches", new JaegerThriftCollectorHandler(strPort,
                handlerFactory, wfSender, traceDisabled::get, spanLogsDisabled::get,
                preprocessors.get(strPort), sampler, traceAlwaysSampleErrors,
                traceJaegerApplicationName, traceDerivedCustomTagKeys));
        server.listen().channel().closeFuture().sync();
        server.shutdown(false);
      } catch (InterruptedException e) {
        logger.info("Listener on port " + strPort + " shut down.");
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Jaeger trace collector exception", e);
      } finally {
        activeListeners.dec();
      }
    }, "listener-jaeger-thrift-" + strPort);
    logger.info("listening on port: " + strPort + " for trace data (Jaeger format)");
  }

  protected void startTraceZipkinListener(String strPort,
                                          ReportableEntityHandlerFactory handlerFactory,
                                          @Nullable WavefrontSender wfSender,
                                          Sampler sampler) {
    final int port = Integer.parseInt(strPort);
    if (httpHealthCheckAllPorts) healthCheckManager.enableHealthcheck(port);
    ChannelHandler channelHandler = new ZipkinPortUnificationHandler(strPort, healthCheckManager,
        handlerFactory, wfSender, traceDisabled::get, spanLogsDisabled::get,
        preprocessors.get(strPort), sampler, traceAlwaysSampleErrors, traceZipkinApplicationName,
        traceDerivedCustomTagKeys);
    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort,
        traceListenerMaxReceivedLength, traceListenerHttpBufferSize, listenerIdleConnectionTimeout),
            port).withChildChannelOptions(childChannelOptions), "listener-zipkin-trace-" + port);
    logger.info("listening on port: " + strPort + " for trace data (Zipkin format)");
  }

  @VisibleForTesting
  protected void startGraphiteListener(String strPort,
                                       ReportableEntityHandlerFactory handlerFactory,
                                       SharedGraphiteHostAnnotator hostAnnotator, boolean isDelta) {
    final int port = Integer.parseInt(strPort);
    registerPrefixFilter(strPort);
    registerTimestampFilter(strPort);
    if (httpHealthCheckAllPorts) healthCheckManager.enableHealthcheck(port);

    WavefrontPortUnificationHandler wavefrontPortUnificationHandler =
            new WavefrontPortUnificationHandler(strPort, tokenAuthenticator, healthCheckManager, DECODERS,
                    handlerFactory, hostAnnotator, preprocessors.get(strPort), isDelta);

    startAsManagedThread(new TcpIngester(createInitializer(wavefrontPortUnificationHandler, strPort,
        pushListenerMaxReceivedLength, pushListenerHttpBufferSize, listenerIdleConnectionTimeout),
        port).withChildChannelOptions(childChannelOptions), "listener-graphite-" + port);
  }

  @VisibleForTesting
  protected void startRelayListener(String strPort,
                                    ReportableEntityHandlerFactory handlerFactory,
                                    SharedGraphiteHostAnnotator hostAnnotator) {
    final int port = Integer.parseInt(strPort);
    registerPrefixFilter(strPort);
    registerTimestampFilter(strPort);
    if (httpHealthCheckAllPorts) healthCheckManager.enableHealthcheck(port);

    Map<ReportableEntityType, ReportableEntityDecoder> filteredDecoders = decoderSupplier.get().
        entrySet().stream().filter(x -> !x.getKey().equals(ReportableEntityType.SOURCE_TAG)).
        collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    ChannelHandler channelHandler = new RelayPortUnificationHandler(strPort, tokenAuthenticator,
        healthCheckManager, filteredDecoders, handlerFactory, preprocessors.get(strPort),
        hostAnnotator, histogramDisabled::get, traceDisabled::get, spanLogsDisabled::get);
    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort,
        pushListenerMaxReceivedLength, pushListenerHttpBufferSize, listenerIdleConnectionTimeout),
        port).withChildChannelOptions(childChannelOptions), "listener-relay-" + port);
  }

  protected void startLogsIngestionListener(int port, LogsIngester logsIngester) {
    if (tokenAuthenticator.authRequired()) {
      logger.warning("Filebeat log ingestion is not compatible with HTTP authentication, ignoring");
      return;
    }
    final Server filebeatServer = new Server("0.0.0.0", port, listenerIdleConnectionTimeout,
        Runtime.getRuntime().availableProcessors());
    filebeatServer.setMessageListener(new FilebeatIngester(logsIngester,
        System::currentTimeMillis));
    startAsManagedThread(() -> {
      try {
        activeListeners.inc();
        filebeatServer.listen();
      } catch (InterruptedException e) {
        logger.log(Level.SEVERE, "Filebeat server interrupted.", e);
      } catch (Exception e) {
        // ChannelFuture throws undeclared checked exceptions, so we need to handle it
        //noinspection ConstantConditions
        if (e instanceof BindException) {
          bindErrors.inc();
          logger.severe("Unable to start listener - port " + port + " is already in use!");
        } else {
          logger.log(Level.SEVERE, "Filebeat exception", e);
        }
      } finally {
        activeListeners.dec();
      }
    }, "listener-logs-filebeat-" + port);
    logger.info("listening on port: " + port + " for Filebeat logs");
  }

  @VisibleForTesting
  protected void startRawLogsIngestionListener(int port, LogsIngester logsIngester) {
    String strPort = String.valueOf(port);
    if (httpHealthCheckAllPorts) healthCheckManager.enableHealthcheck(port);
    ChannelHandler channelHandler = new RawLogsIngesterPortUnificationHandler(strPort, logsIngester,
        hostnameResolver, tokenAuthenticator, healthCheckManager, preprocessors.get(strPort));

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort,
        rawLogsMaxReceivedLength, rawLogsHttpBufferSize, listenerIdleConnectionTimeout), port).
            withChildChannelOptions(childChannelOptions), "listener-logs-raw-" + port);
    logger.info("listening on port: " + strPort + " for raw logs");
  }

  @VisibleForTesting
  protected void startAdminListener(int port) {
    ChannelHandler channelHandler = new AdminPortUnificationHandler(tokenAuthenticator,
        healthCheckManager, String.valueOf(port), adminApiRemoteIpWhitelistRegex);

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, String.valueOf(port),
        pushListenerMaxReceivedLength, pushListenerHttpBufferSize, listenerIdleConnectionTimeout),
            port).withChildChannelOptions(childChannelOptions),
        "listener-http-admin-" + port);
    logger.info("Admin port: " + port);
  }

  @VisibleForTesting
  protected void startHealthCheckListener(int port) {
    healthCheckManager.enableHealthcheck(port);
    ChannelHandler channelHandler = new HttpHealthCheckEndpointHandler(healthCheckManager, port);

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, String.valueOf(port),
        pushListenerMaxReceivedLength, pushListenerHttpBufferSize, listenerIdleConnectionTimeout),
            port).withChildChannelOptions(childChannelOptions),
        "listener-http-healthcheck-" + port);
    logger.info("Health check port enabled: " + port);
  }


  protected void startHistogramListeners(Iterator<String> ports,
                                         ReportableEntityHandler<ReportPoint> pointHandler,
                                         SharedGraphiteHostAnnotator hostAnnotator,
                                         @Nullable Utils.Granularity granularity,
                                         int flushSecs, boolean memoryCacheEnabled,
                                         File baseDirectory, Long accumulatorSize, int avgKeyBytes,
                                         int avgDigestBytes, short compression) {
    String listenerBinType = Utils.Granularity.granularityToString(granularity);
    // Accumulator
    MapLoader<HistogramKey, AgentDigest, HistogramKeyMarshaller, AgentDigestMarshaller> mapLoader =
        new MapLoader<>(
        HistogramKey.class,
        AgentDigest.class,
        accumulatorSize,
        avgKeyBytes,
        avgDigestBytes,
        HistogramKeyMarshaller.get(),
        AgentDigestMarshaller.get(),
        persistAccumulator);

    File accumulationFile = new File(baseDirectory, "accumulator." + listenerBinType);
    ChronicleMap<HistogramKey, AgentDigest> accumulator = mapLoader.get(accumulationFile);

    histogramExecutor.scheduleWithFixedDelay(
        () -> {
          // warn if accumulator is more than 1.5x the original size,
          // as ChronicleMap starts losing efficiency
          if (accumulator.size() > accumulatorSize * 5) {
            logger.severe("Histogram " + listenerBinType + " accumulator size (" +
                accumulator.size() + ") is more than 5x higher than currently configured size (" +
                accumulatorSize + "), which may cause severe performance degradation issues " +
                "or data loss! If the data volume is expected to stay at this level, we strongly " +
                "recommend increasing the value for accumulator size in wavefront.conf and " +
                "restarting the proxy.");
          } else if (accumulator.size() > accumulatorSize * 2) {
            logger.warning("Histogram " + listenerBinType + " accumulator size (" +
                accumulator.size() + ") is more than 2x higher than currently configured size (" +
                accumulatorSize + "), which may cause performance issues. If the data volume is " +
                "expected to stay at this level, we strongly recommend increasing the value " +
                "for accumulator size in wavefront.conf and restarting the proxy.");
          }
        },
        10,
        10,
        TimeUnit.SECONDS);

    Accumulator cachedAccumulator = new AccumulationCache(accumulator,
        (memoryCacheEnabled ? accumulatorSize : 0), null);

    // Schedule write-backs
    histogramExecutor.scheduleWithFixedDelay(
        cachedAccumulator::flush,
        histogramAccumulatorResolveInterval,
        histogramAccumulatorResolveInterval,
        TimeUnit.MILLISECONDS);

    PointHandlerDispatcher dispatcher = new PointHandlerDispatcher(cachedAccumulator, pointHandler,
        histogramAccumulatorFlushMaxBatchSize < 0 ? null : histogramAccumulatorFlushMaxBatchSize,
        granularity);

    histogramExecutor.scheduleWithFixedDelay(dispatcher, histogramAccumulatorFlushInterval,
        histogramAccumulatorFlushInterval, TimeUnit.MILLISECONDS);

    // gracefully shutdown persisted accumulator (ChronicleMap) on proxy exit
    shutdownTasks.add(() -> {
      try {
        logger.fine("Flushing in-flight histogram accumulator digests: " + listenerBinType);
        cachedAccumulator.flush();
        logger.fine("Shutting down histogram accumulator cache: " + listenerBinType);
        accumulator.close();
      } catch (Throwable t) {
        logger.log(Level.SEVERE, "Error flushing " + listenerBinType +
            " accumulator, possibly unclean shutdown: ", t);
      }
    });

    ReportableEntityHandlerFactory histogramHandlerFactory = new ReportableEntityHandlerFactory() {
      private Map<HandlerKey, ReportableEntityHandler> handlers = new HashMap<>();
        @Override
      public ReportableEntityHandler getHandler(HandlerKey handlerKey) {
          return handlers.computeIfAbsent(handlerKey, k -> new HistogramAccumulationHandlerImpl(
              handlerKey.getHandle(), cachedAccumulator, pushBlockedSamples,
              TimeUnit.SECONDS.toMillis(flushSecs), granularity, compression,
              () -> validationConfiguration, granularity == null));
      }
    };

    ports.forEachRemaining(port -> {
      registerPrefixFilter(port);
      registerTimestampFilter(port);
      if (httpHealthCheckAllPorts) healthCheckManager.enableHealthcheck(Integer.parseInt(port));

      WavefrontPortUnificationHandler wavefrontPortUnificationHandler =
          new WavefrontPortUnificationHandler(port, tokenAuthenticator, healthCheckManager,
              decoderSupplier.get(), histogramHandlerFactory, hostAnnotator,
              preprocessors.get(port), false);
      startAsManagedThread(new TcpIngester(createInitializer(wavefrontPortUnificationHandler, port,
          histogramMaxReceivedLength, histogramHttpBufferSize, listenerIdleConnectionTimeout),
          Integer.parseInt(port)).withChildChannelOptions(childChannelOptions),
          "listener-histogram-" + port);
      logger.info("listening on port: " + port + " for histogram samples, accumulating to the " +
          listenerBinType);
    });

  }

  private static ChannelInitializer createInitializer(ChannelHandler channelHandler,
                                                      String port, int messageMaxLength,
                                                      int httpRequestBufferSize, int idleTimeout) {
    return createInitializer(ImmutableList.of(() -> new PlainTextOrHttpFrameDecoder(channelHandler,
        messageMaxLength, httpRequestBufferSize)), port, idleTimeout);
  }

  private static ChannelInitializer createInitializer(
      Iterable<Supplier<ChannelHandler>> channelHandlerSuppliers, String port, int idleTimeout) {
    ChannelHandler idleStateEventHandler = new IdleStateEventHandler(Metrics.newCounter(
        new TaggedMetricName("listeners", "connections.idle.closed", "port", port)));
    ChannelHandler connectionTracker = new ConnectionTrackingHandler(
        Metrics.newCounter(new TaggedMetricName("listeners", "connections.accepted", "port",
            port)),
        Metrics.newCounter(new TaggedMetricName("listeners", "connections.active", "port",
            port)));
    return new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addFirst("idlehandler", new IdleStateHandler(idleTimeout, 0, 0));
        pipeline.addLast("idlestateeventhandler", idleStateEventHandler);
        pipeline.addLast("connectiontracker", connectionTracker);
        channelHandlerSuppliers.forEach(x -> pipeline.addLast(x.get()));
      }
    };
  }

  private Iterator<String> portIterator(@Nullable String inputString) {
    return inputString == null ?
        Collections.emptyIterator() :
        Splitter.on(",").omitEmptyStrings().trimResults().split(inputString).iterator();
  }

  private void registerTimestampFilter(String strPort) {
    preprocessors.getSystemPreprocessor(strPort).forReportPoint().addFilter(
        new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));
  }

  private void registerPrefixFilter(String strPort) {
    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.getSystemPreprocessor(strPort).forReportPoint().
          addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
  }

  /**
   * Push agent configuration during check-in by the collector.
   *
   * @param config The configuration to process.
   */
  @Override
  protected void processConfiguration(AgentConfiguration config) {
    try {
      agentAPI.proxyConfigProcessed(agentId);
      Long pointsPerBatch = config.getPointsPerBatch();
      if (BooleanUtils.isTrue(config.getCollectorSetsPointsPerBatch())) {
        if (pointsPerBatch != null) {
          // if the collector is in charge and it provided a setting, use it
          pushFlushMaxPoints.set(pointsPerBatch.intValue());
          logger.fine("Proxy push batch set to (remotely) " + pointsPerBatch);
        } // otherwise don't change the setting
      } else {
        // restores the agent setting
        pushFlushMaxPoints.set(pushFlushMaxPointsInitialValue);
        logger.fine("Proxy push batch set to (locally) " + pushFlushMaxPoints.get());
      }

      if (BooleanUtils.isTrue(config.getCollectorSetsRateLimit())) {
        Long collectorRateLimit = config.getCollectorRateLimit();
        if (pushRateLimiter != null && collectorRateLimit != null &&
            pushRateLimiter.getRate() != collectorRateLimit) {
          pushRateLimiter.setRate(collectorRateLimit);
          logger.warning("Proxy rate limit set to " + collectorRateLimit + " remotely");
        }
      } else {
        if (pushRateLimiter != null && pushRateLimiter.getRate() != pushRateLimit) {
          pushRateLimiter.setRate(pushRateLimit);
          if (pushRateLimit >= UNLIMITED) {
            logger.warning("Proxy rate limit no longer enforced by remote");
          } else {
            logger.warning("Proxy rate limit restored to " + pushRateLimit);
          }
        }
      }

      if (BooleanUtils.isTrue(config.getCollectorSetsRetryBackoff())) {
        if (config.getRetryBackoffBaseSeconds() != null) {
          // if the collector is in charge and it provided a setting, use it
          retryBackoffBaseSeconds.set(config.getRetryBackoffBaseSeconds());
          logger.fine("Proxy backoff base set to (remotely) " +
              config.getRetryBackoffBaseSeconds());
        } // otherwise don't change the setting
      } else {
        // restores the agent setting
        retryBackoffBaseSeconds.set(retryBackoffBaseSecondsInitialValue);
        logger.fine("Proxy backoff base set to (locally) " + retryBackoffBaseSeconds.get());
      }

      histogramDisabled.set(BooleanUtils.toBoolean(config.getHistogramDisabled()));
      traceDisabled.set(BooleanUtils.toBoolean(config.getTraceDisabled()));
      spanLogsDisabled.set(BooleanUtils.toBoolean(config.getSpanLogsDisabled()));
    } catch (RuntimeException e) {
      // cannot throw or else configuration update thread would die.
    }
  }

  protected void startAsManagedThread(Runnable target, @Nullable String threadName) {
    Thread thread = new Thread(target);
    if (threadName != null) {
      thread.setName(threadName);
    }
    managedThreads.add(thread);
    thread.start();
  }

  @Override
  public void stopListeners() {
    managedThreads.forEach(Thread::interrupt);
    managedThreads.forEach(thread -> {
      try {
        thread.join(TimeUnit.SECONDS.toMillis(10));
      } catch (InterruptedException e) {
        // ignore
      }
    });
  }
}
