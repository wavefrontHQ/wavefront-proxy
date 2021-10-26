package com.wavefront.agent.logforwarder.ingestion.restapi;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 8/25/21 5:16 PM
 */
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import io.vertx.ext.web.RoutingContext;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;//TODO port this to use ProxyConfig
import com.wavefront.agent.logforwarder.constants.LogForwarderUris;
import com.wavefront.agent.logforwarder.ingestion.constants.IngestConstants;
import com.wavefront.agent.logforwarder.constants.LogForwarderConstants;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.buffer.memory.PayLoadInMemoryBuffer;
import com.wavefront.agent.logforwarder.ingestion.processors.config.ComponentConfig;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventPayload;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser.StructureFactory;
import com.wavefront.agent.logforwarder.ingestion.util.RequestUtil;

/**
 * Logfowarder's REST based ingestion endpoint for logs.
 * POST method submits incoming post body to a in memory queue to be processed asynchronously by
 * the {@link PayLoadInMemoryBuffer}
 */
public class LogForwarderRestIngestEndpoint implements BaseHttpEndpoint {

  public static final String SELF_LINK = LogForwarderUris.LOF_FORWARDER_INGEST_URI;
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private PayLoadInMemoryBuffer payloadInMemoryBuffer;
//TODO Metrics revisit this
//  private Meter payloadInBytesMeter = MetricsService
//      .getInstance()
//      .getMeter(MetricRegistry.name(getClass().getSimpleName(), MetricsService.PAYLOADSIZE_IN_BYTES));
//
//  private Meter rateMeter = MetricsService
//      .getInstance()
//      .getMeter(MetricRegistry.name(getClass().getSimpleName(), MetricsService.COUNT));

  public LogForwarderRestIngestEndpoint(String componentName) {
    ComponentConfig componentConfig = LogForwarderConfigProperties.componentConfigMap.get(componentName);
    payloadInMemoryBuffer = new PayLoadInMemoryBuffer(componentConfig);
    payloadInMemoryBuffer.dequeueMessagesAndSend();
    logger.info("Started LogForwarderRestIngestEndpoint in path : " + LogForwarderUris.LOF_FORWARDER_INGEST_URI);
  }

  @Override
  public void handlePost(CompletableFuture future, RoutingContext routingContext) {
    processPost(future, routingContext);
  }

  private void processPost(CompletableFuture future, RoutingContext routingContext) {
    String body = routingContext.getBody().toString();
//    rateMeter.mark(); TODO Fix me metrics
    long contentLength = body == null ? 0 : body.length();
//    payloadInBytesMeter.mark(contentLength);// TODO Fix me metrics

    EventPayload eventPayload = EventPayload.createEventPayload(body, "TENANT", "ORG",
        EventPayload.PayloadType.LOGS, contentLength, new HashMap<>(), false);

    eventPayload.requestHeaders.put(IngestConstants.STRUCTURE_HEADER, StructureFactory.Structure.SIMPLE.name());

    boolean isAddSuccessful = payloadInMemoryBuffer.addEventPayloadToQueue(eventPayload);

    if (isAddSuccessful) {
      RequestUtil.setResponse(future, null, LogForwarderConstants.SUCCESS_JSON_MSG,
          ContentType.APPLICATION_JSON.toString(), new HashMap<>());
    } else {
      RuntimeException ex = new RuntimeException("Server is Busy");
      RequestUtil.setException(future, null, ex, HttpStatus.SC_SERVICE_UNAVAILABLE, ex.getMessage());
    }
  }
}