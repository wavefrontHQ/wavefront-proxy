package com.wavefront.agent;

import com.google.common.base.Preconditions;
import com.wavefront.agent.formatter.Formatter;
import com.wavefront.agent.formatter.GraphiteFormatter;
import com.wavefront.ingester.graphite.GraphiteIngester;
import io.netty.channel.ChannelHandler;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Push-only Agent.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class PushAgent extends AbstractAgent {

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
    for (String strPort : pushListenerPorts.split(",")) {
      startListener(strPort, null);
    }
    if (graphitePorts != null) {
      Preconditions.checkNotNull(graphiteFormat, "graphiteFormat must be supplied to enable graphite support");
      Preconditions.checkNotNull(graphiteDelimiters, "graphiteDelimiters must be supplied to enable graphite support");
      for (String strPort : graphitePorts.split(",")) {
        if (strPort.trim().length() > 0) {
          GraphiteFormatter formatter = new GraphiteFormatter(graphiteFormat, graphiteDelimiters);
          startListener(strPort, formatter);
          logger.info("listening on port: " + strPort + " for graphite metrics");
        }
      }
    }
    if (httpJsonPorts != null) {
      for (String strPort : httpJsonPorts.split(",")) {
        if (strPort.trim().length() > 0) {
          try {
            int port = Integer.parseInt(strPort);
            // will immediately start the server.
            JettyHttpContainerFactory.createServer(
                new URI("http://localhost:" + strPort + "/"),
                new ResourceConfig(JacksonFeature.class).
                    register(new JsonMetricsEndpoint(agentAPI, agentId, port, hostname, prefix, pushLogLevel,
                        pushValidationLevel, pushFlushInterval, pushBlockedSamples, pushFlushMaxPoints)),
                true);
            logger.info("listening on port: " + strPort + " for HTTP JSON metrics");
          } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to bind to: " + strPort + " for HTTP JSON metrics", e);
          }
        }
      }
    }
  }

  protected void startListener(String strPort, Formatter formatter) {
    int port = Integer.parseInt(strPort);

    // Set up a custom graphite handler, with no formatter
    ChannelHandler graphiteHandler = new GraphiteStringHandler(agentAPI, agentId, port, prefix,
        pushLogLevel, pushValidationLevel, pushFlushInterval, pushFlushMaxPoints, pushBlockedSamples, formatter);

    // Start the graphite head in a new thread; if we have a formatter, then set implicitHosts to false to avoid
    // tagging with the wrong host in the GraphiteIngester pipeline
    new Thread(new GraphiteIngester(graphiteHandler, port, false, formatter == null)).start();
  }
}
