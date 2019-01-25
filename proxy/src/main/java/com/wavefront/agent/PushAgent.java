package com.wavefront.agent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.tdunning.math.stats.AgentDigest.AgentDigestMarshaller;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.channels.Connection;
import com.wavefront.agent.channel.CachingGraphiteHostAnnotator;
import com.wavefront.agent.channel.ConnectionTrackingHandler;
import com.wavefront.agent.channel.IdleStateEventHandler;
import com.wavefront.agent.channel.PlainTextOrHttpFrameDecoder;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.formatter.GraphiteFormatter;
import com.wavefront.agent.handlers.InternalProxyWavefrontClient;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactoryImpl;
import com.wavefront.agent.handlers.SenderTaskFactory;
import com.wavefront.agent.handlers.SenderTaskFactoryImpl;
import com.wavefront.agent.histogram.HistogramLineIngester;
import com.wavefront.agent.histogram.MapLoader;
import com.wavefront.agent.histogram.PointHandlerDispatcher;
import com.wavefront.agent.histogram.QueuingChannelHandler;
import com.wavefront.agent.histogram.Utils;
import com.wavefront.agent.histogram.Utils.HistogramKey;
import com.wavefront.agent.histogram.Utils.HistogramKeyMarshaller;
import com.wavefront.agent.histogram.accumulator.AccumulationCache;
import com.wavefront.agent.histogram.accumulator.AccumulationTask;
import com.wavefront.agent.histogram.tape.TapeDeck;
import com.wavefront.agent.histogram.tape.TapeStringListConverter;
import com.wavefront.agent.listeners.ChannelByteArrayHandler;
import com.wavefront.agent.listeners.DataDogPortUnificationHandler;
import com.wavefront.agent.listeners.JsonMetricsEndpoint;
import com.wavefront.agent.listeners.OpenTSDBPortUnificationHandler;
import com.wavefront.agent.listeners.RelayPortUnificationHandler;
import com.wavefront.agent.listeners.WavefrontPortUnificationHandler;
import com.wavefront.agent.listeners.WriteHttpJsonMetricsEndpoint;
import com.wavefront.agent.listeners.tracing.JaegerThriftCollectorHandler;
import com.wavefront.agent.listeners.tracing.TracePortUnificationHandler;
import com.wavefront.agent.listeners.tracing.ZipkinPortUnificationHandler;
import com.wavefront.agent.logsharvesting.FilebeatIngester;
import com.wavefront.agent.logsharvesting.LogsIngester;
import com.wavefront.agent.logsharvesting.RawLogsIngester;
import com.wavefront.agent.preprocessor.ReportPointAddPrefixTransformer;
import com.wavefront.agent.preprocessor.ReportPointTimestampInRangeFilter;
import com.wavefront.agent.sampler.SpanSamplerUtils;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.api.agent.Constants;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.data.Validation;
import com.wavefront.ingester.Decoder;
import com.wavefront.ingester.GraphiteDecoder;
import com.wavefront.ingester.HistogramDecoder;
import com.wavefront.ingester.OpenTSDBDecoder;
import com.wavefront.ingester.PickleProtocolDecoder;
import com.wavefront.ingester.ReportPointDecoderWrapper;
import com.wavefront.ingester.ReportSourceTagDecoder;
import com.wavefront.ingester.ReportableEntityDecoder;
import com.wavefront.ingester.SpanDecoder;
import com.wavefront.ingester.StreamIngester;
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
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import wavefront.report.ReportPoint;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Push-only Agent.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class PushAgent extends AbstractAgent {

  protected final List<Thread> managedThreads = new ArrayList<>();
  protected final IdentityHashMap<ChannelOption<?>, Object> childChannelOptions = new IdentityHashMap<>();
  protected ScheduledExecutorService histogramExecutor;
  protected ScheduledExecutorService histogramScanExecutor;
  protected ScheduledExecutorService histogramFlushExecutor;
  protected final Counter bindErrors = Metrics.newCounter(ExpectedAgentMetric.LISTENERS_BIND_ERRORS.metricName);
  private volatile ReportableEntityDecoder<String, ReportPoint> wavefrontDecoder;
  protected CachingGraphiteHostAnnotator remoteHostAnnotator;
  protected SenderTaskFactory senderTaskFactory;
  protected ReportableEntityHandlerFactory handlerFactory;

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
        wavefrontDecoder = new ReportPointDecoderWrapper(new GraphiteDecoder("unknown", customSourceTags));
      }
      return wavefrontDecoder;
    }
  }

  @Override
  protected void startListeners() {
    if (soLingerTime >= 0) {
      childChannelOptions.put(ChannelOption.SO_LINGER, soLingerTime);
    }
    remoteHostAnnotator = new CachingGraphiteHostAnnotator(customSourceTags, disableRdnsLookup);
    senderTaskFactory = new SenderTaskFactoryImpl(agentAPI, agentId, pushRateLimiter,
        pushFlushInterval, pushFlushMaxPoints, pushMemoryBufferLimit);
    handlerFactory = new ReportableEntityHandlerFactoryImpl(senderTaskFactory, pushBlockedSamples, flushThreads);

    if (pushListenerPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(pushListenerPorts);
      for (String strPort : ports) {
        startGraphiteListener(strPort, handlerFactory, remoteHostAnnotator);
        logger.info("listening on port: " + strPort + " for Wavefront metrics");
      }
    }

    {
      // Histogram bootstrap.
      Iterator<String> histMinPorts = Strings.isNullOrEmpty(histogramMinuteListenerPorts) ?
          Collections.emptyIterator() :
          Splitter.on(",").omitEmptyStrings().trimResults().split(histogramMinuteListenerPorts).iterator();

      Iterator<String> histHourPorts = Strings.isNullOrEmpty(histogramHourListenerPorts) ?
          Collections.emptyIterator() :
          Splitter.on(",").omitEmptyStrings().trimResults().split(histogramHourListenerPorts).iterator();

      Iterator<String> histDayPorts = Strings.isNullOrEmpty(histogramDayListenerPorts) ?
          Collections.emptyIterator() :
          Splitter.on(",").omitEmptyStrings().trimResults().split(histogramDayListenerPorts).iterator();

      Iterator<String> histDistPorts = Strings.isNullOrEmpty(histogramDistListenerPorts) ?
          Collections.emptyIterator() :
          Splitter.on(",").omitEmptyStrings().trimResults().split(histogramDistListenerPorts).iterator();

      int activeHistogramAggregationTypes = (histDayPorts.hasNext() ? 1 : 0) + (histHourPorts.hasNext() ? 1 : 0) +
          (histMinPorts.hasNext() ? 1 : 0) + (histDistPorts.hasNext() ? 1 : 0);
      if (activeHistogramAggregationTypes > 0) { /*Histograms enabled*/
        histogramExecutor = Executors.newScheduledThreadPool(1 + activeHistogramAggregationTypes,
            new NamedThreadFactory("histogram-service"));
        histogramFlushExecutor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() / 2,
            new NamedThreadFactory("histogram-flush"));
        histogramScanExecutor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() / 2,
            new NamedThreadFactory("histogram-scan"));
        managedExecutors.add(histogramExecutor);
        managedExecutors.add(histogramFlushExecutor);
        managedExecutors.add(histogramScanExecutor);

        File baseDirectory = new File(histogramStateDirectory);
        if (persistMessages || persistAccumulator) {
          // Check directory
          checkArgument(baseDirectory.isDirectory(), baseDirectory.getAbsolutePath() + " must be a directory!");
          checkArgument(baseDirectory.canWrite(), baseDirectory.getAbsolutePath() + " must be write-able!");
        }

        // Central dispatch
        PointHandler histogramHandler = new PointHandlerImpl(
            "histogram ports",
            pushValidationLevel,
            pushBlockedSamples,
            prefix,
            getFlushTasks(Constants.PUSH_FORMAT_HISTOGRAM, "histogram ports"));

        // Input queue factory
        TapeDeck<List<String>> accumulatorDeck = new TapeDeck<>(
            persistMessagesCompression
                ? TapeStringListConverter.getCompressionEnabledInstance()
                : TapeStringListConverter.getDefaultInstance(),
            persistMessages);

        Decoder<String> distributionDecoder = new HistogramDecoder("unknown");
        Decoder<String> graphiteDecoder = new GraphiteDecoder("unknown", customSourceTags);
        if (histMinPorts.hasNext()) {
          startHistogramListeners(histMinPorts, graphiteDecoder, histogramHandler, accumulatorDeck,
              Utils.Granularity.MINUTE, histogramMinuteFlushSecs, histogramMinuteAccumulators,
              histogramMinuteMemoryCache, baseDirectory, histogramMinuteAccumulatorSize, histogramMinuteAvgKeyBytes,
              histogramMinuteAvgDigestBytes, histogramMinuteCompression);
        }

        if (histHourPorts.hasNext()) {
          startHistogramListeners(histHourPorts, graphiteDecoder, histogramHandler, accumulatorDeck,
              Utils.Granularity.HOUR, histogramHourFlushSecs, histogramHourAccumulators,
              histogramHourMemoryCache, baseDirectory, histogramHourAccumulatorSize, histogramHourAvgKeyBytes,
              histogramHourAvgDigestBytes, histogramHourCompression);
        }

        if (histDayPorts.hasNext()) {
          startHistogramListeners(histDayPorts, graphiteDecoder, histogramHandler, accumulatorDeck,
              Utils.Granularity.DAY, histogramDayFlushSecs, histogramDayAccumulators,
              histogramDayMemoryCache, baseDirectory, histogramDayAccumulatorSize, histogramDayAvgKeyBytes,
              histogramDayAvgDigestBytes, histogramDayCompression);
        }

        if (histDistPorts.hasNext()) {
          startHistogramListeners(histDistPorts, distributionDecoder, histogramHandler, accumulatorDeck,
              null, histogramDistFlushSecs, histogramDistAccumulators,
              histogramDistMemoryCache, baseDirectory, histogramDistAccumulatorSize, histogramDistAvgKeyBytes,
              histogramDistAvgDigestBytes, histogramDistCompression);
        }
      }
    }

    if (StringUtils.isNotBlank(graphitePorts) || StringUtils.isNotBlank(picklePorts)) {
      if (tokenAuthenticator.authRequired()) {
        logger.warning("Graphite mode is not compatible with HTTP authentication, ignoring");
      } else {
        Preconditions.checkNotNull(graphiteFormat, "graphiteFormat must be supplied to enable graphite support");
        Preconditions.checkNotNull(graphiteDelimiters, "graphiteDelimiters must be supplied to enable graphite support");
        GraphiteFormatter graphiteFormatter = new GraphiteFormatter(graphiteFormat, graphiteDelimiters,
            graphiteFieldsToRemove);
        Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(graphitePorts);
        for (String strPort : ports) {
          preprocessors.forPort(strPort).forPointLine().addTransformer(0, graphiteFormatter);
          startGraphiteListener(strPort, handlerFactory, null);
          logger.info("listening on port: " + strPort + " for graphite metrics");
        }
        if (picklePorts != null) {
          Splitter.on(",").omitEmptyStrings().trimResults().split(picklePorts).forEach(
              strPort -> {
                PointHandler pointHandler = new PointHandlerImpl(strPort, pushValidationLevel,
                    pushBlockedSamples, getFlushTasks(strPort));
                startPickleListener(strPort, pointHandler, graphiteFormatter);
              }
          );
        }
      }
    }
    if (opentsdbPorts != null) {
      Splitter.on(",").omitEmptyStrings().trimResults().split(opentsdbPorts).forEach(
          strPort -> startOpenTsdbListener(strPort, handlerFactory)
      );
    }
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

      Splitter.on(",").omitEmptyStrings().trimResults().split(dataDogJsonPorts).forEach(
          strPort -> startDataDogListener(strPort, handlerFactory, httpClient)
      );
    }
    if (traceListenerPorts != null) {
      Splitter.on(",").omitEmptyStrings().trimResults().split(traceListenerPorts).forEach(
          strPort -> startTraceListener(strPort, handlerFactory)
      );
    }
    if (traceJaegerListenerPorts != null) {
      Splitter.on(",").omitEmptyStrings().trimResults().split(traceJaegerListenerPorts).forEach(
          strPort -> startTraceJaegerListener(strPort, handlerFactory,
                new InternalProxyWavefrontClient(handlerFactory, strPort))
      );
    }
    if (pushRelayListenerPorts != null) {
      Splitter.on(",").omitEmptyStrings().trimResults().split(pushRelayListenerPorts).forEach(
          strPort -> startRelayListener(strPort, handlerFactory)
      );
    }
    if (traceZipkinListenerPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(traceZipkinListenerPorts);
      for (String strPort : ports) {
        startTraceZipkinListener(strPort, handlerFactory,
            new InternalProxyWavefrontClient(handlerFactory, strPort));
      }
    }
    if (jsonListenerPorts != null) {
      Splitter.on(",").omitEmptyStrings().trimResults().split(jsonListenerPorts).forEach(this::startJsonListener);
    }
    if (writeHttpJsonListenerPorts != null) {
      Splitter.on(",").omitEmptyStrings().trimResults().split(writeHttpJsonListenerPorts).
          forEach(this::startWriteHttpJsonListener);
    }

    // Logs ingestion.
    if (loadLogsIngestionConfig() != null) {
      logger.info("Loading logs ingestion.");
      PointHandler pointHandler = new PointHandlerImpl("logs-ingester", pushValidationLevel, pushBlockedSamples,
          getFlushTasks("logs-ingester"));
      startLogsIngestionListeners(filebeatPort, rawLogsPort, pointHandler);
    } else {
      logger.info("Not loading logs ingestion -- no config specified.");
    }
  }

  protected void startJsonListener(String strPort) {
    if (tokenAuthenticator.authRequired()) {
      logger.warning("Port " + strPort + " (jsonListener) is not compatible with HTTP authentication, ignoring");
      return;
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));

    startAsManagedThread(() -> {
      activeListeners.inc();
      try {
        org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server(Integer.parseInt(strPort));
        server.setHandler(new JsonMetricsEndpoint(strPort, hostname, prefix,
            pushValidationLevel, pushBlockedSamples, getFlushTasks(strPort), preprocessors.forPort(strPort)));
        server.start();
        server.join();
      } catch (InterruptedException e) {
        logger.warning("Http Json server interrupted.");
      } catch (Exception e) {
        if (e instanceof BindException) {
          bindErrors.inc();
          logger.severe("Unable to start listener - port " + String.valueOf(strPort) + " is already in use!");
        } else {
          logger.log(Level.SEVERE, "HttpJson exception", e);
        }
      } finally {
        activeListeners.dec();
      }
    }, "listener-plaintext-json-" + strPort);
  }

  protected void startWriteHttpJsonListener(String strPort) {
    if (tokenAuthenticator.authRequired()) {
      logger.warning("Port " + strPort + " (writeHttpJson) is not compatible with HTTP authentication, ignoring");
      return;
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));

    startAsManagedThread(() -> {
      activeListeners.inc();
      try {
        org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server(Integer.parseInt(strPort));
        server.setHandler(new WriteHttpJsonMetricsEndpoint(strPort, hostname, prefix,
            pushValidationLevel, pushBlockedSamples, getFlushTasks(strPort), preprocessors.forPort(strPort)));
        server.start();
        server.join();
      } catch (InterruptedException e) {
        logger.warning("WriteHttpJson server interrupted.");
      } catch (Exception e) {
        if (e instanceof BindException) {
          bindErrors.inc();
          logger.severe("Unable to start listener - port " + String.valueOf(strPort) + " is already in use!");
        } else {
          logger.log(Level.SEVERE, "WriteHttpJson exception", e);
        }
      } finally {
        activeListeners.dec();
      }
    }, "listener-plaintext-writehttpjson-" + strPort);
  }

  protected void startLogsIngestionListeners(int portFilebeat, int portRawLogs, PointHandler pointHandler) {
    if (tokenAuthenticator.authRequired()) {
      logger.warning("Logs ingestion is not compatible with HTTP authentication, ignoring");
      return;
    }
    try {
      final LogsIngester logsIngester = new LogsIngester(pointHandler, this::loadLogsIngestionConfig, prefix,
          System::currentTimeMillis);
      logsIngester.start();

      if (portFilebeat > 0) {
        final Server filebeatServer = new Server(portFilebeat);
        filebeatServer.setMessageListener(new FilebeatIngester(logsIngester, System::currentTimeMillis));
        startAsManagedThread(() -> {
          try {
            activeListeners.inc();
            filebeatServer.listen();
          } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Filebeat server interrupted.", e);
          } catch (Exception e) {
            // ChannelFuture throws undeclared checked exceptions, so we need to handle it
            if (e instanceof BindException) {
              bindErrors.inc();
              logger.severe("Unable to start listener - port " + String.valueOf(portRawLogs) + " is already in use!");
            } else {
              logger.log(Level.SEVERE, "Filebeat exception", e);
            }
          } finally {
            activeListeners.dec();
          }
        }, "listener-logs-filebeat-" + portFilebeat);
      }

      if (portRawLogs > 0) {
        RawLogsIngester rawLogsIngester = new RawLogsIngester(logsIngester, portRawLogs, System::currentTimeMillis).
            withChannelIdleTimeout(listenerIdleConnectionTimeout).
            withMaxLength(rawLogsMaxReceivedLength);
        startAsManagedThread(() -> {
          try {
            activeListeners.inc();
            rawLogsIngester.listen();
          } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Raw logs server interrupted.", e);
          } catch (Exception e) {
            // ChannelFuture throws undeclared checked exceptions, so we need to handle it
            if (e instanceof BindException) {
              bindErrors.inc();
              logger.severe("Unable to start listener - port " + String.valueOf(portRawLogs) + " is already in use!");
            } else {
              logger.log(Level.SEVERE, "RawLogs exception", e);
            }
          } finally {
            activeListeners.dec();
          }
        }, "listener-logs-raw-" + portRawLogs);
      }
    } catch (ConfigurationException e) {
      logger.log(Level.SEVERE, "Cannot start logsIngestion", e);
    }
  }

  protected void startOpenTsdbListener(final String strPort, ReportableEntityHandlerFactory handlerFactory) {
    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));
    final int port = Integer.parseInt(strPort);
    ReportableEntityDecoder<String, ReportPoint> openTSDBDecoder = new ReportPointDecoderWrapper(
        new OpenTSDBDecoder("unknown", customSourceTags));

    ChannelHandler channelHandler = new OpenTSDBPortUnificationHandler(strPort, tokenAuthenticator, openTSDBDecoder,
        handlerFactory, preprocessors.forPort(strPort), remoteHostAnnotator);

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort), port)
            .withChildChannelOptions(childChannelOptions), "listener-plaintext-opentsdb-" + port);
    logger.info("listening on port: " + strPort + " for OpenTSDB metrics");
  }

  protected void startDataDogListener(final String strPort, ReportableEntityHandlerFactory handlerFactory,
                                      HttpClient httpClient) {
    if (tokenAuthenticator.authRequired()) {
      logger.warning("Port: " + strPort + " (DataDog) is not compatible with HTTP authentication, ignoring");
      return;
    }
    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));
    final int port = Integer.parseInt(strPort);

    ChannelHandler channelHandler = new DataDogPortUnificationHandler(strPort, handlerFactory,
        dataDogProcessSystemMetrics, dataDogProcessServiceChecks, httpClient, dataDogRequestRelayTarget,
        preprocessors.forPort(strPort));

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort), port)
            .withChildChannelOptions(childChannelOptions), "listener-plaintext-datadog-" + port);
    logger.info("listening on port: " + strPort + " for DataDog metrics");
  }

  protected void startPickleListener(String strPort, PointHandler pointHandler, GraphiteFormatter formatter) {
    if (tokenAuthenticator.authRequired()) {
      logger.warning("Port: " + strPort + " (pickle format) is not compatible with HTTP authentication, ignoring");
      return;
    }
    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));
    int port = Integer.parseInt(strPort);
    // Set up a custom handler
    ChannelHandler channelHandler = new ChannelByteArrayHandler(
        new PickleProtocolDecoder("unknown", customSourceTags, formatter.getMetricMangler(), port),
        pointHandler, preprocessors.forPort(strPort));

    // create a class to use for StreamIngester to get a new FrameDecoder
    // for each request (not shareable since it's storing how many bytes
    // read, etc)
    // the pickle listener for carbon-relay streams data in its own format:
    //   [Length of pickled data to follow in a 4 byte unsigned int]
    //   [pickled data of the given length]
    //   <repeat ...>
    // the LengthFieldBasedFrameDecoder() parses out the length and grabs
    // <length> bytes from the stream and passes that chunk as a byte array
    // to the decoder.
    class FrameDecoderFactoryImpl implements StreamIngester.FrameDecoderFactory {
      @Override
      public ChannelInboundHandler getDecoder() {
        return new LengthFieldBasedFrameDecoder(ByteOrder.BIG_ENDIAN, 1000000, 0, 4, 0, 4, false);
      }
    }

    startAsManagedThread(new StreamIngester(new FrameDecoderFactoryImpl(), channelHandler, port)
        .withChildChannelOptions(childChannelOptions), "listener-binary-pickle-" + port);
    logger.info("listening on port: " + strPort + " for pickle protocol metrics");
  }

  protected void startTraceListener(final String strPort, ReportableEntityHandlerFactory handlerFactory) {
    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));
    final int port = Integer.parseInt(strPort);

    Sampler rateSampler = SpanSamplerUtils.getRateSampler(traceSamplingRate);
    Sampler durationSampler = SpanSamplerUtils.getDurationSampler(traceSamplingDuration);
    List<Sampler> samplers = SpanSamplerUtils.fromSamplers(rateSampler, durationSampler);
    Sampler compositeSampler = new CompositeSampler(samplers);

    ChannelHandler channelHandler = new TracePortUnificationHandler(strPort, tokenAuthenticator,
        new SpanDecoder("unknown"), preprocessors.forPort(strPort), handlerFactory, compositeSampler);

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort), port)
        .withChildChannelOptions(childChannelOptions), "listener-plaintext-trace-" + port);
    logger.info("listening on port: " + strPort + " for trace data");
  }

  protected void startTraceJaegerListener(
      String strPort,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable WavefrontSender wfSender) {
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
            register("Collector::submitBatches", new JaegerThriftCollectorHandler(strPort, handlerFactory,
                wfSender, traceDisabled, preprocessors.forPort(strPort)));
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

  protected void startTraceZipkinListener(
      String strPort,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable WavefrontSender wfSender) {
    final int port = Integer.parseInt(strPort);
    ChannelHandler channelHandler = new ZipkinPortUnificationHandler(strPort, handlerFactory, wfSender, traceDisabled,
        preprocessors.forPort(strPort));

    startAsManagedThread(new TcpIngester(createInitializer(channelHandler, strPort), port).
        withChildChannelOptions(childChannelOptions), "listener-zipkin-trace-" + port);
    logger.info("listening on port: " + strPort + " for trace data (Zipkin format)");
  }

  @VisibleForTesting
  protected void startGraphiteListener(
      String strPort,
      ReportableEntityHandlerFactory handlerFactory,
      CachingGraphiteHostAnnotator hostAnnotator) {
    final int port = Integer.parseInt(strPort);

    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));

    Map<ReportableEntityType, ReportableEntityDecoder> decoders = ImmutableMap.of(
        ReportableEntityType.POINT, getDecoderInstance(),
        ReportableEntityType.SOURCE_TAG, new ReportSourceTagDecoder(),
        ReportableEntityType.HISTOGRAM, new ReportPointDecoderWrapper(new HistogramDecoder("unknown")));
    WavefrontPortUnificationHandler wavefrontPortUnificationHandler = new WavefrontPortUnificationHandler(strPort,
        tokenAuthenticator, decoders, handlerFactory, hostAnnotator, preprocessors.forPort(strPort));
    startAsManagedThread(
        new TcpIngester(createInitializer(wavefrontPortUnificationHandler, strPort), port).
            withChildChannelOptions(childChannelOptions), "listener-graphite-" + port);
  }

  @VisibleForTesting
  protected void startRelayListener(String strPort, ReportableEntityHandlerFactory handlerFactory) {
    final int port = Integer.parseInt(strPort);

    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));

    Map<ReportableEntityType, ReportableEntityDecoder> decoders = ImmutableMap.of(
        ReportableEntityType.POINT, getDecoderInstance(),
        ReportableEntityType.HISTOGRAM, new ReportPointDecoderWrapper(new HistogramDecoder("unknown")));
    ChannelHandler channelHandler = new RelayPortUnificationHandler(strPort, tokenAuthenticator, decoders,
        handlerFactory, preprocessors.forPort(strPort));
    startAsManagedThread(
        new TcpIngester(createInitializer(channelHandler, strPort), port).
            withChildChannelOptions(childChannelOptions), "listener-relay-" + port);
  }

  protected void startHistogramListeners(Iterator<String> ports, Decoder<String> decoder, PointHandler pointHandler,
                                         TapeDeck<List<String>> receiveDeck, @Nullable Utils.Granularity granularity,
                                         int flushSecs, int fanout, boolean memoryCacheEnabled, File baseDirectory,
                                         Long accumulatorSize, int avgKeyBytes, int avgDigestBytes, short compression) {
    if (tokenAuthenticator.authRequired()) {
      logger.warning("Histograms are not compatible with HTTP authentication, ignoring");
      return;
    }
    String listenerBinType = Utils.Granularity.granularityToString(granularity);
    // Accumulator
    MapLoader<HistogramKey, AgentDigest, HistogramKeyMarshaller, AgentDigestMarshaller> mapLoader = new MapLoader<>(
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
          // warn if accumulator is more than 1.5x the original size, as ChronicleMap starts losing efficiency
          if (accumulator.size() > accumulatorSize * 5) {
            logger.severe("Histogram " + listenerBinType + " accumulator size (" + accumulator.size() +
                ") is more than 5x higher than currently configured size (" + accumulatorSize +
                "), which may cause severe performance degradation issues or data loss! " +
                "If the data volume is expected to stay at this level, we strongly recommend increasing the value " +
                "for accumulator size in wavefront.conf and restarting the proxy.");
          } else if (accumulator.size() > accumulatorSize * 2) {
            logger.warning("Histogram " + listenerBinType + " accumulator size (" + accumulator.size() +
                ") is more than 2x higher than currently configured size (" + accumulatorSize +
                "), which may cause performance issues. " +
                "If the data volume is expected to stay at this level, we strongly recommend increasing the value " +
                "for accumulator size in wavefront.conf and restarting the proxy.");
          }
        },
        10,
        10,
        TimeUnit.SECONDS);

    AccumulationCache cachedAccumulator = new AccumulationCache(accumulator,
        (memoryCacheEnabled ? accumulatorSize : 0), null);

    // Schedule write-backs
    histogramExecutor.scheduleWithFixedDelay(
        cachedAccumulator.getResolveTask(),
        histogramAccumulatorResolveInterval,
        histogramAccumulatorResolveInterval,
        TimeUnit.MILLISECONDS);

    PointHandlerDispatcher dispatcher = new PointHandlerDispatcher(cachedAccumulator, pointHandler,
        histogramAccumulatorFlushMaxBatchSize < 0 ? null : histogramAccumulatorFlushMaxBatchSize, granularity);

    histogramExecutor.scheduleWithFixedDelay(dispatcher, histogramAccumulatorFlushInterval,
        histogramAccumulatorFlushInterval, TimeUnit.MILLISECONDS);

    // gracefully shutdown persisted accumulator (ChronicleMap) on proxy exit
    shutdownTasks.add(() -> {
      try {
        logger.fine("Flushing in-flight histogram accumulator digests: " + listenerBinType);
        cachedAccumulator.getResolveTask().run();
        logger.fine("Shutting down histogram accumulator cache: " + listenerBinType);
        accumulator.close();
      } catch (Throwable t) {
        logger.log(Level.SEVERE, "Error flushing " + listenerBinType + " accumulator, possibly unclean shutdown: ", t);
      }
    });

    ports.forEachRemaining(port -> {
      startHistogramListener(
          port,
          decoder,
          pointHandler,
          cachedAccumulator,
          baseDirectory,
          granularity,
          receiveDeck,
          TimeUnit.SECONDS.toMillis(flushSecs),
          fanout,
          compression
      );
      logger.info("listening on port: " + port + " for histogram samples, accumulating to the " +
          listenerBinType);
    });

  }

  /**
   * Needs to set up a queueing handler and a consumer/lexer for the queue
   */
  private void startHistogramListener(
      String portAsString,
      Decoder<String> decoder,
      PointHandler handler,
      AccumulationCache accumulationCache,
      File directory,
      @Nullable Utils.Granularity granularity,
      TapeDeck<List<String>> receiveDeck,
      long timeToLiveMillis,
      int fanout,
      short compression) {

    int port = Integer.parseInt(portAsString);
    List<ChannelHandler> handlers = new ArrayList<>();

    for (int i = 0; i < fanout; ++i) {
      File tapeFile = new File(directory, "Port_" + portAsString + "_" + i);
      ObjectQueue<List<String>> receiveTape = receiveDeck.getTape(tapeFile);

      // Set-up scanner
      AccumulationTask scanTask = new AccumulationTask(
          receiveTape,
          accumulationCache,
          decoder,
          handler,
          Validation.Level.valueOf(pushValidationLevel),
          timeToLiveMillis,
          granularity,
          compression);

      histogramScanExecutor.scheduleWithFixedDelay(scanTask,
          histogramProcessingQueueScanInterval, histogramProcessingQueueScanInterval, TimeUnit.MILLISECONDS);

      QueuingChannelHandler<String> inputHandler = new QueuingChannelHandler<>(receiveTape,
          pushFlushMaxPoints.get(), histogramDisabled);
      handlers.add(inputHandler);
      histogramFlushExecutor.scheduleWithFixedDelay(inputHandler.getBufferFlushTask(),
          histogramReceiveBufferFlushInterval, histogramReceiveBufferFlushInterval, TimeUnit.MILLISECONDS);
    }

    // Set-up producer
    startAsManagedThread(new HistogramLineIngester(handlers, port).
            withChannelIdleTimeout(listenerIdleConnectionTimeout).
            withMaxLength(histogramMaxReceivedLength),
        "listener-plaintext-histogram-" + port);
  }

  private ChannelInitializer createInitializer(ChannelHandler channelHandler, String strPort) {
    ChannelHandler idleStateEventHandler = new IdleStateEventHandler(
        Metrics.newCounter(new TaggedMetricName("listeners", "connections.idle.closed", "port", strPort)));
    ChannelHandler connectionTracker = new ConnectionTrackingHandler(
        Metrics.newCounter(new TaggedMetricName("listeners", "connections.accepted", "port", strPort)),
        Metrics.newCounter(new TaggedMetricName("listeners", "connections.active", "port", strPort)));
    return new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addFirst("idlehandler", new IdleStateHandler(listenerIdleConnectionTimeout, 0, 0));
        pipeline.addLast("idlestateeventhandler", idleStateEventHandler);
        pipeline.addLast("connectiontracker", connectionTracker);
        pipeline.addLast(new PlainTextOrHttpFrameDecoder(channelHandler, pushListenerMaxReceivedLength,
            pushListenerHttpBufferSize));
      }
    };
  }

  /**
   * Push agent configuration during check-in by the collector.
   *
   * @param config The configuration to process.
   */
  @Override
  protected void processConfiguration(AgentConfiguration config) {
    try {
      agentAPI.agentConfigProcessed(agentId);
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
        if (pushRateLimiter != null && collectorRateLimit != null && pushRateLimiter.getRate() != collectorRateLimit) {
          pushRateLimiter.setRate(collectorRateLimit);
          logger.warning("Proxy rate limit set to " + collectorRateLimit + " remotely");
        }
      } else {
        if (pushRateLimiter != null && pushRateLimiter.getRate() != pushRateLimit) {
          pushRateLimiter.setRate(pushRateLimit);
          if (pushRateLimit >= 10_000_000) {
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
    for (Thread thread : managedThreads) {
      thread.interrupt();
      try {
        thread.join(TimeUnit.SECONDS.toMillis(10));
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }
}
