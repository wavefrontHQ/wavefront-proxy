package com.wavefront.agent;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import com.beust.jcommander.internal.Lists;
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
import com.wavefront.ingester.Decoder;
import com.wavefront.ingester.GraphiteDecoder;
import com.wavefront.ingester.GraphiteHostAnnotator;
import com.wavefront.ingester.HistogramDecoder;
import com.wavefront.ingester.OpenTSDBDecoder;
import com.wavefront.ingester.PickleProtocolDecoder;
import com.wavefront.ingester.StreamIngester;
import com.wavefront.ingester.StringLineIngester;
import com.wavefront.ingester.TcpIngester;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.logstash.beats.Server;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.annotation.Nullable;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Push-only Agent.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class PushAgent extends AbstractAgent {

  protected final List<Thread> managedThreads = new ArrayList<>();
  protected final IdentityHashMap<ChannelOption<?>, Object> childChannelOptions = new IdentityHashMap<>();

  public static void main(String[] args) throws IOException {
    // Start the ssh daemon
    new PushAgent().start(args);
  }

  public PushAgent() {
    super(false, true);
  }

  protected PushAgent(boolean reportAsPushAgent) {
    super(false, reportAsPushAgent);
  }

  @Override
  protected void startListeners() {
    if (soLingerTime >= 0) {
      childChannelOptions.put(ChannelOption.SO_LINGER, 0);
    }
    if (pushListenerPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(pushListenerPorts);
      for (String strPort : ports) {
        startGraphiteListener(strPort, false);
      }
    }

    {
      // Histogram bootstrap.
      Iterator<String> histMinPorts = Strings.isNullOrEmpty(histogramMinsListenerPorts) ?
          Collections.emptyIterator() :
          Splitter.on(",").omitEmptyStrings().trimResults().split(histogramMinsListenerPorts).iterator();

      Iterator<String> histHourPorts = Strings.isNullOrEmpty(histogramHoursListenerPorts) ?
          Collections.emptyIterator() :
          Splitter.on(",").omitEmptyStrings().trimResults().split(histogramHoursListenerPorts).iterator();

      Iterator<String> histDayPorts = Strings.isNullOrEmpty(histogramDaysListenerPorts) ?
          Collections.emptyIterator() :
          Splitter.on(",").omitEmptyStrings().trimResults().split(histogramDaysListenerPorts).iterator();

      Iterator<String> histDistPorts = Strings.isNullOrEmpty(histogramDistListenerPorts) ?
          Collections.emptyIterator() :
          Splitter.on(",").omitEmptyStrings().trimResults().split(histogramDistListenerPorts).iterator();

      if /*Histograms enabled*/ (
          histDayPorts.hasNext()
              || histHourPorts.hasNext()
              || histMinPorts.hasNext()
              || histDistPorts.hasNext()) {
        if (histogramCompression < 20 || histogramCompression > 1000) {
          logger.log(Level.WARNING, "Histogram compression (" +
              histogramCompression + ") outside of supported range [20;1000], will be clamped.");
          histogramCompression = (short) Math.min(1000, (short) Math.max(20, histogramCompression));
        }

        File baseDirectory = new File(histogramStateDirectory);
        if (persistMessages || persistAccumulator) {
          // Check directory
          checkArgument(baseDirectory.isDirectory(), baseDirectory.getAbsolutePath() + " must be a directory!");
          checkArgument(baseDirectory.canWrite(), baseDirectory.getAbsolutePath() + " must be write-able!");
        }

        // Accumulator
        MapLoader<HistogramKey, AgentDigest, HistogramKeyMarshaller, AgentDigestMarshaller> mapLoader = new MapLoader<>(
            HistogramKey.class,
            AgentDigest.class,
            histogramAccumulatorSize,
            avgHistogramKeyBytes,
            avgHistogramDigestBytes,
            HistogramKeyMarshaller.get(),
            AgentDigestMarshaller.get(),
            persistAccumulator);

        File accumulationFile = new File(baseDirectory, "accumulator");
        ConcurrentMap<HistogramKey, AgentDigest> accumulator = mapLoader.get(accumulationFile);

        AccumulationCache cachedAccumulator = new AccumulationCache(accumulator, histogramAccumulatorSize, null);
        // Schedule write-backs
        histogramExecutor.scheduleWithFixedDelay(
            cachedAccumulator.getResolveTask(),
            histogramAccumulatorResolveInterval,
            histogramAccumulatorResolveInterval,
            TimeUnit.MILLISECONDS);

        // Central dispatch
        PointHandler histogramHandler = new PointHandlerImpl(
            "histogram ports",
            pushValidationLevel,
            pushBlockedSamples,
            prefix,
            getFlushTasks(Constants.PUSH_FORMAT_HISTOGRAM, "histogram ports"));
        PointHandlerDispatcher dispatchTask = new PointHandlerDispatcher(accumulator, histogramHandler);
        histogramExecutor.scheduleWithFixedDelay(dispatchTask, 100L, 1L, TimeUnit.MICROSECONDS);

        // Input queue factory
        TapeDeck<List<String>> accumulatorDeck = new TapeDeck<>(TapeStringListConverter.get(), persistMessages);

        // Decoders
        Decoder<String> sampleDecoder = new GraphiteDecoder("unknown", customSourceTags);
        Decoder<String> distributionDecoder = new HistogramDecoder("unknown");

        // Minute ports...
        histMinPorts.forEachRemaining(port -> {
          startHistogramListener(
              port,
              sampleDecoder,
              histogramHandler,
              cachedAccumulator,
              baseDirectory,
              Utils.Granularity.MINUTE,
              accumulatorDeck,
              TimeUnit.SECONDS.toMillis(histogramMinuteFlushSecs),
              histogramMinuteAccumulators
          );
          logger.info("listening on port: " + port + " for histogram samples, accumulating to the minute");
        });

        // Hour ports...
        histHourPorts.forEachRemaining(port -> {
          startHistogramListener(
              port,
              sampleDecoder,
              histogramHandler,
              cachedAccumulator,
              baseDirectory,
              Utils.Granularity.HOUR,
              accumulatorDeck,
              TimeUnit.SECONDS.toMillis(histogramHourFlushSecs),
              histogramHourAccumulators
          );
          logger.info("listening on port: " + port + " for histogram samples, accumulating to the hour");
        });

        // Day ports...
        histDayPorts.forEachRemaining(port -> {
          startHistogramListener(
              port,
              sampleDecoder,
              histogramHandler,
              cachedAccumulator,
              baseDirectory,
              Utils.Granularity.DAY,
              accumulatorDeck,
              TimeUnit.SECONDS.toMillis(histogramDayFlushSecs),
              histogramDayAccumulators
          );
          logger.info("listening on port: " + port + " for histogram samples, accumulating to the day");
        });

        // Distribution ports...
        histDistPorts.forEachRemaining(port -> {
          startHistogramListener(
              port,
              distributionDecoder,
              histogramHandler,
              cachedAccumulator,
              baseDirectory,
              Utils.Granularity.DAY, // Ignored...
              accumulatorDeck,
              TimeUnit.SECONDS.toMillis(histogramDistFlushSecs),
              histogramDistAccumulators
          );
          logger.info("listening on port: " + port + " for histogram samples, accumulating to the day");
        });
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
            .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours));
        try {
          // will immediately start the server.
          JettyHttpContainerFactory.createServer(
              new URI("http://localhost:" + strPort + "/"),
              new ResourceConfig(JacksonFeature.class).
                  register(new JsonMetricsEndpoint(strPort, hostname, prefix,
                      pushValidationLevel, pushBlockedSamples, getFlushTasks(strPort), preprocessors.forPort(strPort))),
              true);
          logger.info("listening on port: " + strPort + " for HTTP JSON metrics");
        } catch (URISyntaxException e) {
          throw new RuntimeException("Unable to bind to: " + strPort + " for HTTP JSON metrics", e);
        }
      }
    }
    if (writeHttpJsonPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(writeHttpJsonPorts);
      for (String strPort : ports) {
        preprocessors.forPort(strPort).forReportPoint()
            .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours));

        try {
          // will immediately start the server.
          JettyHttpContainerFactory.createServer(
              new URI("http://localhost:" + strPort + "/"),
              new ResourceConfig(JacksonFeature.class).
                  register(new WriteHttpJsonMetricsEndpoint(strPort, hostname, prefix,
                      pushValidationLevel, pushBlockedSamples, getFlushTasks(strPort), preprocessors.forPort(strPort))),
              true);
          logger.info("listening on port: " + strPort + " for Write HTTP JSON metrics");
        } catch (URISyntaxException e) {
          throw new RuntimeException("Unable to bind to: " + strPort + " for Write HTTP JSON metrics", e);
        }
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

        if (filebeatPort > 0) {
          final Server filebeatServer = new Server(filebeatPort);
          filebeatServer.setMessageListener(new FilebeatIngester(logsIngester, System::currentTimeMillis));
          startAsManagedThread(() -> {
            try {
              filebeatServer.listen();
            } catch (InterruptedException e) {
              logger.log(Level.SEVERE, "Filebeat server interrupted.", e);
            }
          });
        }

        if (rawLogsPort > 0) {
          RawLogsIngester rawLogsIngester = new RawLogsIngester(logsIngester, rawLogsPort, System::currentTimeMillis);
          startAsManagedThread(() -> {
            try {
              rawLogsIngester.listen();
            } catch (InterruptedException e) {
              logger.log(Level.SEVERE, "Raw logs server interrupted.", e);
            }
          });
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
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours));
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

        pipeline.addLast(new PlainTextOrHttpFrameDecoder(handler));
      }
    };
    startAsManagedThread(new TcpIngester(initializer, port).withChildChannelOptions(childChannelOptions));
  }

  protected void startPickleListener(String strPort, GraphiteFormatter formatter) {
    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours));
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
        .withChildChannelOptions(childChannelOptions));
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
    startAsManagedThread(new StringLineIngester(channelHandler, port).withChildChannelOptions(childChannelOptions));
  }

  protected void startGraphiteListener(String strPort, boolean withCustomFormatter) {
    int port = Integer.parseInt(strPort);

    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours));
    // Set up a custom graphite handler, with no formatter
    ChannelHandler graphiteHandler = new ChannelStringHandler(
        new GraphiteDecoder("unknown", customSourceTags),
        new PointHandlerImpl(strPort, pushValidationLevel, pushBlockedSamples, getFlushTasks(strPort)),
        preprocessors.forPort(strPort));

    if (!withCustomFormatter) {
      List<Function<Channel, ChannelHandler>> handler = Lists.newArrayList(1);
      handler.add(new Function<Channel, ChannelHandler>() {
        @Override
        public ChannelHandler apply(Channel input) {
          SocketChannel ch = (SocketChannel) input;
          return new GraphiteHostAnnotator(ch.remoteAddress().getHostName(), customSourceTags);
        }
      });
      startAsManagedThread(new StringLineIngester(handler, graphiteHandler, port)
          .withChildChannelOptions(childChannelOptions));
    } else {
      startAsManagedThread(new StringLineIngester(graphiteHandler, port)
          .withChildChannelOptions(childChannelOptions));
    }
  }

  /**
   * Needs to set up a queueing handler and a consumer/lexer for the queue
   */
  protected void startHistogramListener(
      String portAsString,
      Decoder<String> decoder,
      PointHandler handler,
      AccumulationCache accumulationCache,
      File directory,
      Utils.Granularity granularity,
      TapeDeck<List<String>> receiveDeck,
      long timeToLiveMillis,
      int fanout) {

    int port = Integer.parseInt(portAsString);
    List<ChannelHandler> handlers = new ArrayList<>();

    for (int i = 0; i < fanout; ++i) {
      File tapeFile = new File(directory, "Port_" + portAsString + "_" + i);
      ObjectQueue<List<String>> receiveTape = receiveDeck.getTape(tapeFile);

      // Set-up scanner
      AccumulationTask scanTask = new AccumulationTask(
          receiveTape,
          accumulationCache.getCache().asMap(),
          decoder,
          handler,
          Validation.Level.valueOf(pushValidationLevel),
          timeToLiveMillis,
          granularity,
          histogramCompression);

      histogramExecutor.scheduleWithFixedDelay(scanTask, 100L, 1L, TimeUnit.MICROSECONDS);

      QueuingChannelHandler<String> inputHandler = new QueuingChannelHandler<>(receiveTape, 100);
      handlers.add(inputHandler);
      histogramExecutor.scheduleWithFixedDelay(inputHandler.getBufferFlushTask(), 85L, 1L, TimeUnit.MICROSECONDS);
    }

    // Set-up producer
    new Thread(
        new HistogramLineIngester(
            handlers,
            port)).start();
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
          QueuedAgentService.setSplitBatchSize(pointsPerBatch.intValue());
          PostPushDataTimedTask.setPointsPerBatch(pointsPerBatch.intValue());
          logger.fine("Agent push batch set to (remotely) " + pointsPerBatch);
        } // otherwise don't change the setting
      } else {
        // restores the agent setting
        QueuedAgentService.setSplitBatchSize(pushFlushMaxPoints);
        PostPushDataTimedTask.setPointsPerBatch(pushFlushMaxPoints);
        logger.fine("Agent push batch set to (locally) " + pushFlushMaxPoints);
      }

      if (config.getCollectorSetsRetryBackoff() != null &&
          config.getCollectorSetsRetryBackoff()) {
        if (config.getRetryBackoffBaseSeconds() != null) {
          // if the collector is in charge and it provided a setting, use it
          QueuedAgentService.setRetryBackoffBaseSeconds(config.getRetryBackoffBaseSeconds());
          logger.fine("Agent backoff base set to (remotely) " +
                config.getRetryBackoffBaseSeconds());
        } // otherwise don't change the setting
      } else {
        // restores the agent setting
        QueuedAgentService.setRetryBackoffBaseSeconds(retryBackoffBaseSeconds);
        logger.fine("Agent backoff base set to (locally) " + retryBackoffBaseSeconds);
      }
    } catch (RuntimeException e) {
      // cannot throw or else configuration update thread would die.
    }
  }

  protected void startAsManagedThread(Runnable target) {
    Thread thread = new Thread(target);
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
