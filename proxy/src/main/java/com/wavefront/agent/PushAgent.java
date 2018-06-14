package com.wavefront.agent;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.tdunning.math.stats.AgentDigest.AgentDigestMarshaller;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.formatter.GraphiteFormatter;
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
import com.wavefront.agent.logsharvesting.FilebeatIngester;
import com.wavefront.agent.logsharvesting.LogsIngester;
import com.wavefront.agent.logsharvesting.RawLogsIngester;
import com.wavefront.agent.preprocessor.PointPreprocessor;
import com.wavefront.agent.preprocessor.ReportPointAddPrefixTransformer;
import com.wavefront.agent.preprocessor.ReportPointTimestampInRangeFilter;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.api.agent.Constants;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.ingester.Decoder;
import com.wavefront.ingester.GraphiteDecoder;
import com.wavefront.ingester.GraphiteHostAnnotator;
import com.wavefront.ingester.HistogramDecoder;
import com.wavefront.ingester.OpenTSDBDecoder;
import com.wavefront.ingester.PickleProtocolDecoder;
import com.wavefront.ingester.StreamIngester;
import com.wavefront.ingester.StringLineIngester;
import com.wavefront.ingester.TcpIngester;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import net.openhft.chronicle.map.ChronicleMap;

import org.apache.commons.lang.BooleanUtils;
import org.logstash.beats.Server;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
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

  /**
   * Maintain a short-term cache for reverse DNS lookup results to avoid spamming DNS servers
   */
  protected final LoadingCache<InetAddress, String> reverseDnsCache = Caffeine.newBuilder()
      .maximumSize(5000)
      .refreshAfterWrite(5, TimeUnit.MINUTES)
      .build(InetAddress::getHostName);

  public static void main(String[] args) throws IOException {
    // Start the ssh daemon
    new PushAgent().start(args);
  }

  public PushAgent() {
    super(false, true);

    Metrics.newGauge(ExpectedAgentMetric.RDNS_CACHE_SIZE.metricName, new Gauge<Long>() {
      @Override
      public Long value() {
        return reverseDnsCache.estimatedSize();
      }
    });
  }

  @Deprecated
  protected PushAgent(boolean reportAsPushAgent) {
    super(false, reportAsPushAgent);
  }

  @Override
  protected void startListeners() {
    if (soLingerTime >= 0) {
      childChannelOptions.put(ChannelOption.SO_LINGER, soLingerTime);
    }
    if (pushListenerPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(pushListenerPorts);
      for (String strPort : ports) {
        startGraphiteListener(strPort, false);
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

        // Decoders
        Decoder<String> sampleDecoder = new GraphiteDecoder("unknown", customSourceTags);
        Decoder<String> distributionDecoder = new HistogramDecoder("unknown");

        if (histMinPorts.hasNext()) {
          startHistogramListeners(histMinPorts, sampleDecoder, histogramHandler, accumulatorDeck, "minute",
              histogramMinuteFlushSecs, histogramMinuteAccumulators, histogramMinuteMemoryCache, baseDirectory,
              histogramMinuteAccumulatorSize, histogramMinuteAvgKeyBytes, histogramMinuteAvgDigestBytes,
              histogramMinuteCompression);
        }

        if (histHourPorts.hasNext()) {
          startHistogramListeners(histHourPorts, sampleDecoder, histogramHandler, accumulatorDeck, "hour",
              histogramHourFlushSecs, histogramHourAccumulators, histogramHourMemoryCache, baseDirectory,
              histogramHourAccumulatorSize, histogramHourAvgKeyBytes, histogramHourAvgDigestBytes,
              histogramHourCompression);
        }

        if (histDayPorts.hasNext()) {
          startHistogramListeners(histDayPorts, sampleDecoder, histogramHandler, accumulatorDeck, "day",
              histogramDayFlushSecs, histogramDayAccumulators, histogramDayMemoryCache, baseDirectory,
              histogramDayAccumulatorSize, histogramDayAvgKeyBytes, histogramDayAvgDigestBytes,
              histogramDayCompression);
        }

        if (histDistPorts.hasNext()) {
          startHistogramListeners(histDistPorts, distributionDecoder, histogramHandler, accumulatorDeck, "distribution",
              histogramDistFlushSecs, histogramDistAccumulators, histogramDistMemoryCache, baseDirectory,
              histogramDistAccumulatorSize, histogramDistAvgKeyBytes, histogramDistAvgDigestBytes,
              histogramDistCompression);
        }
      }
    }

    GraphiteFormatter graphiteFormatter = null;
    if (graphitePorts != null || picklePorts != null) {
      Preconditions.checkNotNull(graphiteFormat, "graphiteFormat must be supplied to enable graphite support");
      Preconditions.checkNotNull(graphiteDelimiters, "graphiteDelimiters must be supplied to enable graphite support");
      graphiteFormatter = new GraphiteFormatter(graphiteFormat, graphiteDelimiters, graphiteFieldsToRemove);
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(graphitePorts);
      for (String strPort : ports) {
        preprocessors.forPort(strPort).forPointLine().addTransformer(0, graphiteFormatter);
        startGraphiteListener(strPort, true);
        logger.info("listening on port: " + strPort + " for graphite metrics");
      }
    }
    if (opentsdbPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(opentsdbPorts);
      for (String strPort : ports) {
        startOpenTsdbListener(strPort);
        logger.info("listening on port: " + strPort + " for OpenTSDB metrics");
      }
    }
    if (picklePorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(picklePorts);
      for (String strPort : ports) {
        startPickleListener(strPort, graphiteFormatter);
        logger.info("listening on port: " + strPort + " for pickle protocol metrics");
      }
    }
    if (httpJsonPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(httpJsonPorts);
      for (String strPort : ports) {
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
                  logger.severe("Unable to start listener - port " + String.valueOf(strPort) + " is already in use!");
                }
              } finally {
                activeListeners.dec();
              }
            },
            "listener-plaintext-json-" + strPort);
      }
    }
    if (writeHttpJsonPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(writeHttpJsonPorts);
      for (String strPort : ports) {
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
                  logger.severe("Unable to start listener - port " + String.valueOf(strPort) + " is already in use!");
                }
              } finally {
                activeListeners.dec();
              }
            },
            "listener-plaintext-writehttpjson-" + strPort);
      }
    }

    // Logs ingestion.
    if (loadLogsIngestionConfig() != null) {
      logger.info("Loading logs ingestion.");
      try {
        final LogsIngester logsIngester = new LogsIngester(
            new PointHandlerImpl(
                "logs-ingester", pushValidationLevel, pushBlockedSamples, getFlushTasks("logs-ingester")),
            this::loadLogsIngestionConfig, prefix, System::currentTimeMillis);
        logsIngester.start();

        if (filebeatPort > 0) {
          final Server filebeatServer = new Server(filebeatPort);
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
                logger.severe("Unable to start listener - port " + String.valueOf(rawLogsPort) + " is already in use!");
              }
            } finally {
              activeListeners.dec();
            }
          }, "listener-logs-filebeat-" + filebeatPort);
        }

        if (rawLogsPort > 0) {
          RawLogsIngester rawLogsIngester = new RawLogsIngester(logsIngester, rawLogsPort, System::currentTimeMillis).
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
                logger.severe("Unable to start listener - port " + String.valueOf(rawLogsPort) + " is already in use!");
              }
            } finally {
              activeListeners.dec();
            }
          }, "listener-logs-raw-" + rawLogsPort);
        }
      } catch (ConfigurationException e) {
        logger.log(Level.SEVERE, "Cannot start logsIngestion", e);
      }
    } else {
      logger.info("Not loading logs ingestion -- no config specified.");
    }

  }

  protected void startOpenTsdbListener(final String strPort) {
    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));
    final int port = Integer.parseInt(strPort);
    final PostPushDataTimedTask[] flushTasks = getFlushTasks(strPort);
    ChannelInitializer initializer = new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        final ChannelHandler handler = new OpenTSDBPortUnificationHandler(
            new OpenTSDBDecoder("unknown", customSourceTags),
            new PointHandlerImpl(strPort, pushValidationLevel, pushBlockedSamples, flushTasks),
            preprocessors.forPort(strPort));
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("idlehandler", new IdleStateHandler(listenerIdleConnectionTimeout, 0, 0));
        pipeline.addLast(new PlainTextOrHttpFrameDecoder(handler, pushListenerMaxReceivedLength,
            pushListenerHttpBufferSize));
      }
    };
    startAsManagedThread(new TcpIngester(initializer, port).withChildChannelOptions(childChannelOptions),
        "listener-plaintext-opentsdb-" + port);
  }

  protected void startPickleListener(String strPort, GraphiteFormatter formatter) {
    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));
    int port = Integer.parseInt(strPort);
    // Set up a custom handler
    ChannelHandler handler = new ChannelByteArrayHandler(
        new PickleProtocolDecoder("unknown", customSourceTags, formatter.getMetricMangler(), port),
        new PointHandlerImpl(strPort, pushValidationLevel, pushBlockedSamples, getFlushTasks(strPort)),
        preprocessors.forPort(strPort));

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

    startAsManagedThread(new StreamIngester(new FrameDecoderFactoryImpl(), handler, port)
        .withChildChannelOptions(childChannelOptions), "listener-binary-pickle-" + port);
  }

  /**
   * Registers a custom point handler on a particular port.
   *
   * @param strPort      The port to listen on.
   * @param decoder      The decoder to use.
   * @param pointHandler The handler to handle parsed ReportPoints.
   * @param preprocessor Pre-processor (predicates and transform functions) for every point
   */
  protected void startCustomListener(String strPort, Decoder<String> decoder, PointHandler pointHandler,
                                     @Nullable PointPreprocessor preprocessor) {
    int port = Integer.parseInt(strPort);
    ChannelHandler channelHandler = new ChannelStringHandler(decoder, pointHandler, preprocessor);
    startAsManagedThread(new StringLineIngester(channelHandler, port, pushListenerMaxReceivedLength).
            withChildChannelOptions(childChannelOptions), null);
  }

  protected void startGraphiteListener(String strPort, boolean disableUseRemoteClientAsSource) {
    final int port = Integer.parseInt(strPort);

    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours, dataPrefillCutoffHours));
    final PostPushDataTimedTask[] flushTasks = getFlushTasks(strPort);
    // Add a metadatahandler, to handle @SourceTag, @SourceDescription, etc.
    SourceTagHandler metadataHandler = new SourceTagHandlerImpl(getSourceTagFlushTasks(port));
    // Set up a custom graphite handler, with no formatter
    ChannelHandler graphiteHandler = new ChannelStringHandler(
        new GraphiteDecoder("unknown", customSourceTags),
        new PointHandlerImpl(strPort, pushValidationLevel, pushBlockedSamples, getFlushTasks(strPort)),
        preprocessors.forPort(strPort), metadataHandler);

    ChannelInitializer initializer = new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        final ChannelHandler handler = new WavefrontPortUnificationHandler(
            new GraphiteDecoder("unknown", customSourceTags),
            new PointHandlerImpl(strPort, pushValidationLevel, pushBlockedSamples, flushTasks),
            preprocessors.forPort(strPort), metadataHandler);
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("idle-handler", new IdleStateHandler(listenerIdleConnectionTimeout, 0, 0));
        if (!disableUseRemoteClientAsSource) {
          pipeline.addLast("graphite-host-annotator", new GraphiteHostAnnotator(disableRdnsLookup
              ? ch.remoteAddress().getAddress().getHostAddress()
              : ch.remoteAddress().getHostName(), customSourceTags));
        }
        pipeline.addLast(new PlainTextOrHttpFrameDecoder(handler, pushListenerMaxReceivedLength,
            pushListenerHttpBufferSize));
      }
    };
    startAsManagedThread(new TcpIngester(initializer, port).withChildChannelOptions(childChannelOptions),
        "listener-graphite-" + port);
  }

  protected void startHistogramListeners(Iterator<String> ports, Decoder<String> decoder, PointHandler pointHandler,
                                         TapeDeck<List<String>> receiveDeck, String listenerBinType,
                                         int flushSecs, int fanout, boolean memoryCacheEnabled, File baseDirectory,
                                         Long accumulatorSize, int avgKeyBytes, int avgDigestBytes, short compression) {
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
          if (accumulator.size() > accumulatorSize * 1.5) {
            logger.warning("Histogram " + listenerBinType + " accumulator size (" + accumulator.size() +
                ") is much higher than configured size (" + accumulatorSize +
                "), proxy may experience performance issues or crash!");
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
        histogramAccumulatorFlushMaxBatchSize < 0 ? null : histogramAccumulatorFlushMaxBatchSize);

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
          (listenerBinType.equals("minute")
              ? Utils.Granularity.MINUTE
              : (listenerBinType.equals("hour") ? Utils.Granularity.HOUR : Utils.Granularity.DAY)),
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
      Utils.Granularity granularity,
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
      if (config.getCollectorSetsPointsPerBatch() != null &&
          config.getCollectorSetsPointsPerBatch()) {
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

      if (config.getCollectorSetsRetryBackoff() != null &&
          config.getCollectorSetsRetryBackoff()) {
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
