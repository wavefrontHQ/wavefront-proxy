package com.wavefront.agent;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import com.beust.jcommander.internal.Lists;
import com.wavefront.agent.formatter.GraphiteFormatter;
import com.wavefront.agent.logsharvesting.FilebeatListener;
import com.wavefront.agent.preprocessor.PointPreprocessor;
import com.wavefront.agent.preprocessor.ReportPointAddPrefixTransformer;
import com.wavefront.agent.preprocessor.ReportPointTimestampInRangeFilter;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.ingester.Decoder;
import com.wavefront.ingester.GraphiteDecoder;
import com.wavefront.ingester.GraphiteHostAnnotator;
import com.wavefront.ingester.OpenTSDBDecoder;
import com.wavefront.ingester.PickleProtocolDecoder;
import com.wavefront.ingester.StreamIngester;
import com.wavefront.ingester.StringLineIngester;
import com.wavefront.ingester.TcpIngester;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.logstash.beats.Server;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

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
    GraphiteFormatter graphiteFormatter = null;
    if (graphitePorts != null || picklePorts != null) {
      Preconditions.checkNotNull(graphiteFormat, "graphiteFormat must be supplied to enable graphite support");
      Preconditions.checkNotNull(graphiteDelimiters, "graphiteDelimiters must be supplied to enable graphite support");
      graphiteFormatter = new GraphiteFormatter(graphiteFormat, graphiteDelimiters, graphiteFieldsToRemove);
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(graphitePorts);
      for (String strPort : ports) {
        if (strPort.trim().length() > 0) {
          preprocessors.forPort(strPort).forPointLine().addTransformer(0, graphiteFormatter);
          startGraphiteListener(strPort, true);
          logger.info("listening on port: " + strPort + " for graphite metrics");
        }
      }
    }
    if (opentsdbPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(opentsdbPorts);
      for (String strPort : ports) {
        if (strPort.trim().length() > 0) {
          startOpenTsdbListener(strPort);
          logger.info("listening on port: " + strPort + " for OpenTSDB metrics");
        }
      }
    }
    if (picklePorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(picklePorts);
      for (String strPort : ports) {
        if (strPort.trim().length() > 0) {
          startPickleListener(strPort, graphiteFormatter);
          logger.info("listening on port: " + strPort + " for pickle protocol metrics");
        }
      }
    }
    if (httpJsonPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(httpJsonPorts);
      for (String strPort : ports) {
        if (strPort.trim().length() > 0) {
          preprocessors.forPort(strPort).forReportPoint()
              .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours));
          try {
            int port = Integer.parseInt(strPort);
            // will immediately start the server.
            JettyHttpContainerFactory.createServer(
                new URI("http://localhost:" + strPort + "/"),
                new ResourceConfig(JacksonFeature.class).
                    register(new JsonMetricsEndpoint(port, hostname, prefix, pushValidationLevel,
                        pushBlockedSamples, getFlushTasks(port), preprocessors.forPort(strPort))), true);
            logger.info("listening on port: " + strPort + " for HTTP JSON metrics");
          } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to bind to: " + strPort + " for HTTP JSON metrics", e);
          }
        }
      }
    }
    if (writeHttpJsonPorts != null) {
      Iterable<String> ports = Splitter.on(",").omitEmptyStrings().trimResults().split(writeHttpJsonPorts);
      for (String strPort : ports) {
        if (strPort.trim().length() > 0) {
          preprocessors.forPort(strPort).forReportPoint()
              .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours));
          try {
            int port = Integer.parseInt(strPort);
            // will immediately start the server.
            JettyHttpContainerFactory.createServer(
                new URI("http://localhost:" + strPort + "/"),
                new ResourceConfig(JacksonFeature.class).
                    register(new WriteHttpJsonMetricsEndpoint(port, hostname, prefix,
                        pushValidationLevel, pushBlockedSamples, getFlushTasks(port), preprocessors.forPort(strPort))),
                true);
            logger.info("listening on port: " + strPort + " for Write HTTP JSON metrics");
          } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to bind to: " + strPort + " for Write HTTP JSON metrics", e);
          }
        }
      }
    }

    if (filebeatPort != null) {
      final Server filebeatServer = new Server(filebeatPort);
      filebeatServer.setMessageListener(new FilebeatListener(
          new PointHandlerImpl(filebeatPort, pushValidationLevel, pushBlockedSamples, getFlushTasks(filebeatPort)),
          logsIngestionConfig, hostname, prefix, true));
      startAsManagedThread(() -> {
        try {
          filebeatServer.listen();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });
    }
  }

  protected void startOpenTsdbListener(final String strPort) {
    if (prefix != null && !prefix.isEmpty()) {
      preprocessors.forPort(strPort).forReportPoint().addTransformer(new ReportPointAddPrefixTransformer(prefix));
    }
    preprocessors.forPort(strPort).forReportPoint()
        .addFilter(new ReportPointTimestampInRangeFilter(dataBackfillCutoffHours));
    final int port = Integer.parseInt(strPort);
    final PostPushDataTimedTask[] flushTasks = getFlushTasks(port);
    ChannelInitializer initializer = new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        final ChannelHandler handler = new OpenTSDBPortUnificationHandler(
            new OpenTSDBDecoder("unknown", customSourceTags),
            new PointHandlerImpl(port, pushValidationLevel, pushBlockedSamples, flushTasks),
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
        new PointHandlerImpl(port, pushValidationLevel, pushBlockedSamples, getFlushTasks(port)),
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
        new PointHandlerImpl(port, pushValidationLevel, pushBlockedSamples, getFlushTasks(port)),
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
          if (pushLogLevel.equals("DETAILED")) {
            logger.info("Agent push batch set to (remotely) " + pointsPerBatch);
          }
        } // otherwise don't change the setting
      } else {
        // restores the agent setting
        QueuedAgentService.setSplitBatchSize(pushFlushMaxPoints);
        PostPushDataTimedTask.setPointsPerBatch(pushFlushMaxPoints);
        if (pushLogLevel.equals("DETAILED")) {
          logger.info("Agent push batch set to (locally) " + pushFlushMaxPoints);
        }
      }

      if (config.getCollectorSetsRetryBackoff() != null &&
          config.getCollectorSetsRetryBackoff()) {
        if (config.getRetryBackoffBaseSeconds() != null) {
          // if the collector is in charge and it provided a setting, use it
          QueuedAgentService.setRetryBackoffBaseSeconds(config.getRetryBackoffBaseSeconds());
          if (pushLogLevel.equals("DETAILED")) {
            logger.info("Agent backoff base set to (remotely) " +
                config.getRetryBackoffBaseSeconds());
          }
        } // otherwise don't change the setting
      } else {
        // restores the agent setting
        QueuedAgentService.setRetryBackoffBaseSeconds(retryBackoffBaseSeconds);
        if (pushLogLevel.equals("DETAILED")) {
          logger.info("Agent backoff base set to (locally) " + retryBackoffBaseSeconds);
        }
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
