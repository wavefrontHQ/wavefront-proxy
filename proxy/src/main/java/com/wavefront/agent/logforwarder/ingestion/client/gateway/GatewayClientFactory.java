package com.wavefront.agent.logforwarder.ingestion.client.gateway;

import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.verticle.GatewayAgentVerticle;
import com.wavefront.agent.logforwarder.ingestion.restapi.BaseHttpEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * A client factory which holds a single instance which deals with everything to do with creating
 * and managing the ingestion client and injecting configuration properties.
 * to backend including creating the singleton client, failure handling, buffering to disk
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 1:29 PM
 */
public class GatewayClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static GatewayClientFactory instance = new GatewayClientFactory();

  private static final String LEMANS_HOST_URI_FORMAT = "http://localhost:%d";

  private GatewayClientFactory() {

  }

  public static GatewayClientFactory getInstance() {
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
   * @param diskBackedQueueLocation location to write buffers for failure while posting to gateway
   * @throws Throwable
   */
  public void initializeVertxLemansClient(String url, String accessKey, String diskBackedQueueLocation) throws Throwable {
    try {
      if (GatewayClientState.accessKeyVsLemansClient.containsKey(accessKey)) {
        logger.debug(String.format("lemans vertx client already initialized for access key %s", "****"));
        return;
      }
      int port = getLemansClientPort();
      String diskBackQueueLocation = diskBackedQueueLocation;
      if (diskBackQueueLocation != null) {
        diskBackQueueLocation += File.separator + port;
      }
      logger.info("Available number of processors in log-forwarder - " + Runtime.getRuntime()
          .availableProcessors());
      LogForwarderConfigProperties.logForwarderAgentVertx = GatewayAgentVerticle.deploy(
          createLemansAgentServices(), Runtime.getRuntime().availableProcessors(), port);
      GatewayClientManager ingestionClient = startIngestionClient(url, accessKey, port, diskBackQueueLocation);
      GatewayClientState.accessKeyVsLemansClient.put(accessKey, ingestionClient);
    } catch (Exception ex) {
      logger.error("Error while starting lemans client: ", ex);
      throw ex;
    }
  }

  public static Map<String, BaseHttpEndpoint> createLemansAgentServices() {
    //TODO Rethink if the health test endpoint is needed
//    LogForwardingTestConnectionService logForwardingTestConnectionService =
//        new LogForwardingTestConnectionService();

    Map<String, BaseHttpEndpoint> serviceMap = new HashMap<>();
//    serviceMap.put(LogForwardingTestConnectionService.SELF_LINK, logForwardingTestConnectionService);
//    serviceMap.put(LiAgentConfigService.SELF_LINK, liAgentConfigService);
//    serviceMap.put(VsphereConfigService.SELF_LINK, vsphereConfigService);

    return serviceMap;
  }

  /**
   * Start the logs ingestion client host
   *
   * @param url                   log iq url
   * @param accessKey             log iq access key
   * @param diskBackQueueLocation lemans client disk back queue location
   * @return lemans client host
   * @throws Throwable
   */
  private GatewayClientManager startIngestionClient(String url, String accessKey, int port,
                                                    String diskBackQueueLocation) throws Throwable {
    HashMap<String, String> agentProperties = new HashMap<>();
    //WF proxy does not need this feature so command channel is always disabled
    boolean enableCommandChannel = !LogForwarderConfigProperties.logForwarderArgs.disableBidirectionalCommunication;

    logger.info(String.format("lemansServiceUri = %s, lemansAccessKey = %s, " +
        "lemansDataVolume = %s", url, "****", diskBackQueueLocation));

    URI serviceUri = new URI(String.format(LEMANS_HOST_URI_FORMAT, port));
    GatewayClientManager.Builder builder;
    logger.info("Starting ingestion client with access key");
    builder = GatewayClientManager.Builder.forConfig(accessKey, URI.create(url), diskBackQueueLocation);

    builder.withAgentType("log-forwarder")
        .withAgentVersion(1.0)
        .withAgentProperties(agentProperties)
        .shouldEnableTelemetry(false)
        .withMetricsLoggingIntervalMinutes(5)
        .shouldRegisterAgent(enableCommandChannel)
        .withHostServiceUri(serviceUri);

    GatewayClientManager ingestionClient = builder.build();
    ingestionClient.start();

    logger.info(String.format("started log ingestion gateway client host url=%s accessKey=%s " +
        "diskBackQueueLocation=%s", url, "****", diskBackQueueLocation));
    return ingestionClient;
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
    return GatewayClientState.lemansClientLatestPort.incrementAndGet();
  }
}
