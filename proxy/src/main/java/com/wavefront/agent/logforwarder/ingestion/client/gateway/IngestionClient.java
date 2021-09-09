package com.wavefront.agent.logforwarder.ingestion.client.gateway;

import com.vmware.lemans.client.AgentHost;
import com.vmware.lemans.client.gateway.CspTokenConfig;
import com.vmware.lemans.client.gateway.LemansClient;
import com.vmware.log.forwarder.lemansclient.LemansClientState;
import com.vmware.log.forwarder.lemansclient.LogForwarderAgentHost;
import com.vmware.log.forwarder.verticle.LemansAgentVerticle;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 1:29 PM
 */
public class IngestionClient{
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static IngestionClient instance = new IngestionClient();

  private static final String LEMANS_HOST_URI_FORMAT = "http://localhost:%d";

  private IngestionClient() {

  }

  public static IngestionClient getInstance() {
    return instance;
  }

  /**
   * If lemans client is already initialized for the access key, then return
   * <p>
   * Initialize the lemans client for the url/accessKey
   * <p>
   * 1. Determine the lemans disk backed queue location and the port
   * 2. Start the lemans client host
   * 3. Update the mapping related to accessKey/lemans client host
   *
   * @param url       logIq url
   * @param accessKey logIq access key
   * @throws Throwable
   */
  public void initializeVertxLemansClient(String url, String accessKey) throws Throwable {
    try {
      if (LemansClientState.accessKeyVsLemansClient.containsKey(accessKey)) {
        logger.debug(String.format("lemans vertx client already initialized for access key %s", "****"));
        return;
      }
      int port = getLemansClientPort();
      String diskBackQueueLocation = getDiskBackQueueLocation();
      if (diskBackQueueLocation != null) {
        diskBackQueueLocation += File.separator + port;
      }
      logger.info("Available number of processors in log-forwarder - " + Runtime.getRuntime()
          .availableProcessors());
      LogForwarderConfigProperties.logForwarderAgentVertx = LemansAgentVerticle.deploy(LogForwarderAgentHost
          .createLemansAgentServices(), Runtime.getRuntime().availableProcessors(), port);
      LemansClient lemansClient = startLemansClient(url, accessKey, port, diskBackQueueLocation);
      LemansClientState.accessKeyVsLemansClient.put(accessKey, lemansClient);
    } catch (Exception ex) {
      logger.error("Error while starting lemans client: ", ex);
      throw ex;
    }
  }

  private LemansClient startLemansClient(String url, String accessKey, int port,
                                         String diskBackQueueLocation) throws Throwable {
    HashMap<String, String> agentProperties = new HashMap<>();
    boolean enableCommandChannel = !LogForwarderConfigProperties.logForwarderArgs.disableBidirectionalCommunication;

    logger.info(String.format("lemansServiceUri = %s, lemansAccessKey = %s, " +
        "lemansDataVolume = %s", url, "****", diskBackQueueLocation));

    URI serviceUri = new URI(String.format(LEMANS_HOST_URI_FORMAT, port));
    LemansClient.Builder builder;

    //Creating the CspTokenConfig for CSP authentication in le-mans client
    if (accessKey.equalsIgnoreCase(LogForwarderConfigProperties.cspAuthenticationKey)) {
      CspTokenConfig cspTokenConfig = getCspTokenConfig();
      logger.info("Starting le-mans client using csp authentication with cspTokenURI {} and CspTokenContext {} ",
          cspTokenConfig.getCspTokenURI(), cspTokenConfig.getTokenContext());
      builder = LemansClient.Builder.forConfig(null, URI.create(url), diskBackQueueLocation)
          .withCspTokenConfig(cspTokenConfig);
    } else {
      logger.info("Starting le-mans client with access key");
      builder = LemansClient.Builder.forConfig(accessKey, URI.create(url), diskBackQueueLocation);
    }

    builder.withAgentType("log-forwarder")
        .withAgentVersion(1.0)
        .withAgentProperties(agentProperties)
        .shouldEnableTelemetry(false)
        .withMetricsLoggingIntervalMinutes(5)
        .shouldEnableCommandChannel(enableCommandChannel)
        .shouldRegisterAgent(enableCommandChannel)
        .withHostServiceUri(serviceUri);

    builder.withMessageConsumer(messageRequest -> {
      LogForwarderUtils.postToEventForwardingService(serviceUri, messageRequest.body);
    });

    LemansClient lemansClient = builder.build();
    lemansClient.start();

    logger.info(String.format("started lemans client host url=%s accessKey=%s " +
        "diskBackQueueLocation=%s", url, "****", diskBackQueueLocation));
    return lemansClient;
  }

  /**
   * create and get cspTokenConfig from cspTokenUri and cspTokenContext log-forwarder args
   *
   * @return cspTokenConfig required for le-mans client creation
   * @throws URISyntaxException
   */
  private CspTokenConfig getCspTokenConfig() throws URISyntaxException {
    CspTokenConfig cspTokenConfig = new CspTokenConfig();
    if (LogForwarderConfigProperties.logForwarderArgs.cspTokenUri != null) {
      cspTokenConfig.setCspTokenURI(new URI(LogForwarderConfigProperties.logForwarderArgs.cspTokenUri));
    }

    cspTokenConfig.setTokenContext(CspTokenConfig.CspTokenContext.valueOf(LogForwarderConfigProperties
        .logForwarderArgs.cspTokenContext.toUpperCase()));
    return cspTokenConfig;
  }

  /**
   * get the disk back queue location for the port
   *
   * @return disk back queue location
   */
  private String getDiskBackQueueLocation() {
    return LogForwarderConfigProperties.logForwarderArgs.lemansClientDiskBackedQueueLocation;
  }

  /**
   * get lemans client port
   *
   * @return lemans client port
   */
  private int getLemansClientPort() {
    return LemansClientState.lemansClientLatestPort.incrementAndGet();
  }
}
