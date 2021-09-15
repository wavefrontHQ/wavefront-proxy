package com.wavefront.agent.logforwarder.ingestion.processors;


import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.json.simple.JSONAware;
import org.noggit.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.vmware.ingestion.metrics.MetricsService;
import com.vmware.lemans.client.gateway.GatewayClient;
import com.vmware.lemans.client.gateway.GatewayException;
import com.vmware.lemans.client.gateway.GatewayOperation.Action;
import com.vmware.lemans.client.gateway.GatewayRequest;
import com.vmware.lemans.client.gateway.GatewayResponse;
import com.vmware.lemans.client.gateway.LemansClient;
import com.vmware.log.forwarder.systemalerts.SystemAlertUtils;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.vmware.log.forwarder.lemansclient.LemansClientState;
import com.vmware.log.forwarder.lemansclient.LogForwarderAgentHost;
import com.vmware.xenon.common.Operation;// TODO Get rid of this xenon dependency
import com.vmware.xenon.common.Utils;
import com.wavefront.agent.logforwarder.constants.LogForwarderConstants;
import com.wavefront.agent.logforwarder.ingestion.constants.IngestConstants;
import com.wavefront.agent.logforwarder.ingestion.http.client.utils.HttpClientUtils;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventBatch;
import com.wavefront.agent.logforwarder.ingestion.processors.util.JsonUtils;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventPayload;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;


/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 8/31/21 5:22 PM
 */
public abstract class PostToLogIqProcessor implements Processor {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String INGESTION_STREAM = "ingestion-pipeline-stream";
  protected String url;
  protected URI streamUri;
  protected String accessKey;
  protected String tenantIdentifier;
  protected String s3BucketForAuditLogForwarding;
  protected String chainName;
  protected String streamName = INGESTION_STREAM;
  protected int httpTimeOutSecs = 30;
  protected boolean billable = true;
  protected boolean reIngestEnabled = true;
  private AtomicInteger inFlightCount = new AtomicInteger(0);
  protected CloseableHttpAsyncClient httpAsyncClient;

  private MetricsService metricsService = MetricsService.getInstance();

  private Meter bytesPostedMeter = MetricsService
      .getInstance()
      .getMeter(MetricRegistry.name(getClass().getSimpleName(), MetricsService.PAYLOADSIZE_IN_BYTES));

  @Override
  public abstract void initializeProcessor(JSONAware json) throws Throwable;

  @Override
  public EventPayload process(EventPayload eventPayload) throws Exception {
    postToLogIq(eventPayload);
    return eventPayload;
  }

  private Operation getPostOperation(LogForwarderAgentHost lemansClientHost,
                                     EventPayload payload,
                                     String agentId) {
    EventBatch batch = payload.batch;
    String json = JSONUtil.toJSON(batch, 1);
    Operation operation = Operation
        .createPost(lemansClientHost, "/le-mans-client/streams/" + streamName)
        .setBody(json)
        .addRequestHeader("timestamp", Instant.now().toString())
        .addRequestHeader("structure", "default")
        .addRequestHeader("Accept", "application/json")
        .addRequestHeader("Content-Type", "application/json")
        .addRequestHeader("agent", agentId)
        .addRequestHeader(LogForwarderConstants.BILLABLE, String.valueOf(billable))
        .setReferer(lemansClientHost.getUri());

    //Compresses body using gzip before sending to le-mans
    if (Boolean.TRUE.equals(LogForwarderConfigProperties.logForwarderArgs.compressPayload)) {
      operation.addRequestHeader("Content-Encoding", "gzip");
    }

    if (payload.getMetaEvent() == null || payload.getMetaEvent().get(IngestConstants.SYSLOG_PARSED) == null) {
      operation.addRequestHeader("format", "syslog");
    }

    if (StringUtils.isNotEmpty(chainName)) {
      operation.addRequestHeader("chain", chainName);
    }

    if (LogForwarderConfigProperties.orgId != null) {
      if (reIngestEnabled) {
        operation.addRequestHeader("vmc-org-id", LogForwarderConfigProperties.orgId);
      }

      operation.addRequestHeader("lm-tenant-id", Utils.computeHash(LogForwarderConfigProperties.orgId));

      if (StringUtils.isNotEmpty(s3BucketForAuditLogForwarding)
          && StringUtils.isNotEmpty(LogForwarderConfigProperties.logForwarderArgs.region)) {
        operation.addRequestHeader("aws-s3-audit-log-bucket", s3BucketForAuditLogForwarding);
        operation.addRequestHeader("aws-s3-audit-log-region",
            LogForwarderUtils.getAWSRegion(LogForwarderConfigProperties.logForwarderArgs.region).getName());
      }
    }

    if (LogForwarderConfigProperties.logForwarderArgs.sreOrgId != null) {
      operation.addRequestHeader(LogForwarderConstants.HEADER_ARCTIC_SRE_ORG_ID,
          LogForwarderConfigProperties.logForwarderArgs.sreOrgId);
    }

    if (LogForwarderConfigProperties.logForwarderArgs.dimensionMspMasterOrgId != null) {
      operation.addRequestHeader(LogForwarderConstants.DIMENSION_MSP_MASTER_ORG_ID,
          LogForwarderConfigProperties.logForwarderArgs.dimensionMspMasterOrgId);
    }

    return operation;
  }

  GatewayRequest getGatewayRequest(EventPayload payload, String agentId) {
    EventBatch batch = payload.batch;
    String json = JSONUtil.toJSON(batch, 1);

    GatewayRequest gatewayRequest = GatewayRequest.createRequest(Action.POST,
        URI.create("/streams/" + streamName))
        .setBody(json);
    gatewayRequest.addHeader("timestamp", Instant.now().toString());
    gatewayRequest.addHeader("structure", "default");
    gatewayRequest.addHeader("Accept", "application/json");
    gatewayRequest.addHeader("Content-Type", "application/json");
    if (StringUtils.isNotEmpty(agentId)) {
      gatewayRequest.addHeader("agent", agentId);
    }
    gatewayRequest.addHeader(LogForwarderConstants.BILLABLE, String.valueOf(billable));

    // Compresses body using gzip before sending to le-mans
    if (Boolean.TRUE.equals(LogForwarderConfigProperties.logForwarderArgs.compressPayload)) {
      gatewayRequest.addHeader("Content-Encoding", "gzip");
    }

    if (payload.getMetaEvent() == null || payload.getMetaEvent().get(IngestConstants.SYSLOG_PARSED) == null) {
      gatewayRequest.addHeader("format", "syslog");
    }

    if (StringUtils.isNotEmpty(chainName)) {
      gatewayRequest.addHeader("chain", chainName);
    }

    if (LogForwarderConfigProperties.orgId != null) {
      if (reIngestEnabled) {
        gatewayRequest.addHeader("vmc-org-id", LogForwarderConfigProperties.orgId);
      }

      gatewayRequest.addHeader("lm-tenant-id", Utils.computeHash(LogForwarderConfigProperties.orgId));

      if (StringUtils.isNotEmpty(s3BucketForAuditLogForwarding)
          && StringUtils.isNotEmpty(LogForwarderConfigProperties.logForwarderArgs.region)) {
        gatewayRequest.addHeader("aws-s3-audit-log-bucket", s3BucketForAuditLogForwarding);
        gatewayRequest.addHeader("aws-s3-audit-log-region",
            LogForwarderUtils.getAWSRegion(LogForwarderConfigProperties.logForwarderArgs.region).getName());
      }
    }

    if (LogForwarderConfigProperties.logForwarderArgs.sreOrgId != null) {
      gatewayRequest.addHeader(LogForwarderConstants.HEADER_ARCTIC_SRE_ORG_ID,
          LogForwarderConfigProperties.logForwarderArgs.sreOrgId);
    }

    if (LogForwarderConfigProperties.logForwarderArgs.dimensionMspMasterOrgId != null) {
      gatewayRequest.addHeader(LogForwarderConstants.DIMENSION_MSP_MASTER_ORG_ID,
          LogForwarderConfigProperties.logForwarderArgs.dimensionMspMasterOrgId);
    }

    return gatewayRequest;
  }

  private HttpPost getHttpPost(LogForwarderAgentHost lemansClientHost,
                               EventPayload payload,
                               String agentId) {
    EventBatch batch = payload.batch;

    HttpPost httpPost = new HttpPost(streamUri);
    httpPost.addHeader("timestamp", Instant.now().toString());
    httpPost.addHeader("structure", "default");
    httpPost.addHeader("Accept", "application/json");
    httpPost.addHeader("Content-Type", "application/json");
    httpPost.addHeader("agent", agentId);
    httpPost.addHeader(LogForwarderConstants.BILLABLE, String.valueOf(billable));
    httpPost.addHeader("Authorization", "Bearer " + accessKey);

    //Compresses body using gzip before sending to le-mans
    if (Boolean.TRUE.equals(LogForwarderConfigProperties.logForwarderArgs.compressPayload)) {
      httpPost.addHeader("Content-Encoding", "gzip");

      if (Boolean.TRUE.equals(LogForwarderConfigProperties.logForwarderArgs.customSerialization)) {
        JsonUtils.CustomByteArrayOutputStream baos = JsonUtils.toJsonByteStream(payload);
        JsonUtils.CustomByteArrayOutputStream gzos = new JsonUtils.CustomByteArrayOutputStream(HttpClientUtils.BUFFER_SIZE);
        HttpClientUtils.compress(baos.getBuf(), 0, baos.getLength(), gzos);
        httpPost.setEntity(new ByteArrayEntity(gzos.getBuf(), 0, gzos.getLength()));
      } else {
        String json = JSONUtil.toJSON(batch, -1);
        //compareCustomSerialization(payload, json);
        httpPost.setEntity(
            new ByteArrayEntity(
                HttpClientUtils.compress(json)));
      }
    } else {
      String json = JSONUtil.toJSON(batch, -1);
      httpPost.setEntity(new StringEntity(json, StandardCharsets.UTF_8));
    }

    if (payload.getMetaEvent() == null || payload.getMetaEvent().get(IngestConstants.SYSLOG_PARSED) == null) {
      httpPost.addHeader("format", "syslog");
    }

    if (StringUtils.isNotEmpty(chainName)) {
      httpPost.addHeader("chain", chainName);
    }

    if (LogForwarderConfigProperties.orgId != null) {
      if (reIngestEnabled) {
        httpPost.addHeader("vmc-org-id", LogForwarderConfigProperties.orgId);
      }

      httpPost.addHeader("lm-tenant-id", Utils.computeHash(LogForwarderConfigProperties.orgId));

      if (StringUtils.isNotEmpty(s3BucketForAuditLogForwarding)
          && StringUtils.isNotEmpty(LogForwarderConfigProperties.logForwarderArgs.region)) {
        httpPost.addHeader("aws-s3-audit-log-bucket", s3BucketForAuditLogForwarding);
        httpPost.addHeader("aws-s3-audit-log-region",
            LogForwarderUtils.getAWSRegion(LogForwarderConfigProperties.logForwarderArgs.region).getName());
      }
    }

    if (LogForwarderConfigProperties.logForwarderArgs.sreOrgId != null) {
      httpPost.addHeader(LogForwarderConstants.HEADER_ARCTIC_SRE_ORG_ID,
          LogForwarderConfigProperties.logForwarderArgs.sreOrgId);
    }

    if (LogForwarderConfigProperties.logForwarderArgs.dimensionMspMasterOrgId != null) {
      httpPost.addHeader(LogForwarderConstants.DIMENSION_MSP_MASTER_ORG_ID,
          LogForwarderConfigProperties.logForwarderArgs.dimensionMspMasterOrgId);
    }

    return httpPost;
  }

  /**
   * send non kafka payload to lemans client
   *
   * @param payload event payload, not null
   * @throws Exception
   */
  protected void postToLogIq(EventPayload payload) throws Exception {
    int msgsInBlob = payload.batch.size();
    EventBatch batch = payload.batch;

    /* if the in-flight operations count is = threshold value, drop the message to avoid OOMs */
    if ((LogForwarderConfigProperties.inflightOperationsCount > 0) &&
        (inFlightCount.get() > LogForwarderConfigProperties.inflightOperationsCount)) {
      SystemAlertUtils.updateMessagesDroppedMetric(msgsInBlob);
      //TODO Replace MetricService with Proxy's MetricsReporter
      MetricsService.getInstance().getMeter("in-flight-operation-queue-full-blobs-dropped-"
          + tenantIdentifier).mark();
      MetricsService.getInstance().getMeter("in-flight-operation-queue-full-messages-dropped-"
          + tenantIdentifier).mark(msgsInBlob);
//            logger.debug(String.format("in-flight operations max reached, dropping the blob. url=%s, accessKey=%s, " +
//                            "tenantIdentifier=%s, numberOfMessages=%d",
//                    url, accessKey, tenantIdentifier, msgsInBlob));
      return;
    }

    if (Boolean.TRUE.equals(LogForwarderConfigProperties.logForwarderArgs.skipLeMansClient)) {
      postWithAsyncClient(payload);
    } else {
        postWithLeMansClient(payload);
    }
  }

  private void postWithLogIngestionClientHost(EventPayload payload) {
    int msgsInBlob = payload.batch.size();
    LogForwarderAgentHost logIngestionClientHost = LemansClientState.accessKeyVsLemansClientHost.get(accessKey);
    //TODO Operation is a xenon primitive; Get rid of this.
    Operation operation = getPostOperation(logIngestionClientHost, payload, LogForwarderUtils.getForwarderId());

    inFlightCount.incrementAndGet();

    if (httpTimeOutSecs > 0) {
      operation.setExpiration(
          Utils.getSystemNowMicrosUtc() + TimeUnit.SECONDS.toMicros(httpTimeOutSecs));
    }

    long startTime = System.currentTimeMillis();

    if (LogForwarderConfigProperties.logForwarderArgs != null &&
        Boolean.TRUE.equals(LogForwarderConfigProperties.logForwarderArgs.fakePost)) {
      inFlightCount.decrementAndGet();
      updateMetrics(50, true, (System.currentTimeMillis() - startTime), 200);
      return;
    }

    logIngestionClientHost.sendWithDeferredResult(operation).thenAccept((op) -> {
      inFlightCount.decrementAndGet();
      int responseCode = op.getStatusCode();
      boolean postSuccessful = false;
      if ((responseCode >= 200) && (responseCode <= 299)) {
        postSuccessful = true;
      } else {
        logger.error("post to lemans gateway failed responseCode=" + responseCode);
      }
      updateMetrics(msgsInBlob, postSuccessful, (System.currentTimeMillis() - startTime), responseCode);
    }).exceptionally(e -> {
      logger.error("exception while posting data to lemans-gateway(failed)", e);
      inFlightCount.decrementAndGet();
      updateMetrics(msgsInBlob, false, (System.currentTimeMillis() - startTime), -1);
      return null;
    });

  }

  private void postWithLeMansClient(EventPayload payload) {
    int msgsInBlob = payload.batch.size();
    LemansClient lemansClient = LemansClientState.accessKeyVsLemansClient.get(accessKey);
    GatewayClient gatewayClient = lemansClient.getGatewayClient();

    GatewayRequest gatewayRequest = getGatewayRequest(payload, LogForwarderUtils.getForwarderId());

    inFlightCount.incrementAndGet();
    long startTime = System.currentTimeMillis();

    if (LogForwarderConfigProperties.logForwarderArgs != null &&
        Boolean.TRUE.equals(LogForwarderConfigProperties.logForwarderArgs.fakePost)) {
      inFlightCount.decrementAndGet();
      updateMetrics(50, true, (System.currentTimeMillis() - startTime), 200);
      return;
    }

    gatewayClient.sendRequest(gatewayRequest)
        .whenComplete((resp, ex) -> {
          inFlightCount.decrementAndGet();
          if (ex != null) {
            logger.error("exception while posting data to lemans-gateway(failed)", ex);
            int statusCode = -1;
            try {
              if (ex instanceof GatewayException) {
                if (((GatewayException) ex).getGatewayResponse() != null) {
                  GatewayResponse response = ((GatewayException) ex).getGatewayResponse();
                  if (response.getStatusCode() != 0) {
                    statusCode = response.getStatusCode();
                  }
                }
              }
            } catch (Exception ex1) {
              logger.error("Error while fetching status code from Exception", ex);
            }
            updateMetrics(msgsInBlob, false, (System.currentTimeMillis() - startTime),
                statusCode);
          } else {
            int responseCode = resp.getStatusCode();
            boolean postSuccessful = false;
            if ((responseCode >= 200) && (responseCode <= 299)) {
              postSuccessful = true;
            } else {
              logger.error("post to lemans gateway failed responseCode=" + responseCode);
            }
            updateMetrics(msgsInBlob, postSuccessful, (System.currentTimeMillis() - startTime),
                responseCode);
          }

        });
  }

  private void postWithAsyncClient(EventPayload payload) {
    int msgsInBlob = payload.batch.size();
    LogForwarderAgentHost lemansClientHost = LemansClientState.accessKeyVsLemansClientHost.get(accessKey);

    HttpPost operation = getHttpPost(lemansClientHost, payload, LogForwarderUtils.getForwarderId());

    bytesPostedMeter.mark(operation.getEntity().getContentLength());

    inFlightCount.incrementAndGet();


    long startTime = System.currentTimeMillis();

    if (LogForwarderConfigProperties.logForwarderArgs != null &&
        Boolean.TRUE.equals(LogForwarderConfigProperties.logForwarderArgs.fakePost)) {
      inFlightCount.decrementAndGet();
      updateMetrics(msgsInBlob, true, (System.currentTimeMillis() - startTime), 200);
      return;
    }


    httpAsyncClient.execute(operation, new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse httpResponse) {
        inFlightCount.decrementAndGet();

        int responseCode = httpResponse.getStatusLine().getStatusCode();
        boolean postSuccessful = false;
        if ((responseCode >= 200) && (responseCode <= 299)) {
          postSuccessful = true;
        } else {
          logger.error("post to lemans gateway failed responseCode=" + responseCode);
        }
        updateMetrics(msgsInBlob, postSuccessful, (System.currentTimeMillis() - startTime), responseCode);
      }

      @Override
      public void failed(Exception e) {
        logger.error("exception while posting data to lemans-gateway(failed)", e);
        inFlightCount.decrementAndGet();
        updateMetrics(msgsInBlob, false, (System.currentTimeMillis() - startTime), -1);
      }

      @Override
      public void cancelled() {
        logger.error("exception while posting data to lemans-gateway(cancelled)");
        inFlightCount.decrementAndGet();
        updateMetrics(msgsInBlob, false, (System.currentTimeMillis() - startTime), -1);
      }
    });
  }

  private void updateMetrics(int numberOfMessages,
                             boolean postSuccessful,
                             long timeTaken,
                             int responseCode) {
    updatePostToLogIqRelatedMetrics(numberOfMessages, postSuccessful, timeTaken, responseCode);
  }

  /**
   * update the POST to log-iq related metrics
   *
   * @param numberOfMessages number of messages
   * @param postSuccessful   is POST successful
   * @param timeTaken        time taken
   * @param responseCode     response code
   */
  public void updatePostToLogIqRelatedMetrics(
      int numberOfMessages,
      boolean postSuccessful,
      long timeTaken,
      int responseCode) {
    if (postSuccessful) {
      metricsService.getHistogram("messages-per-blob-"
          + tenantIdentifier).update(numberOfMessages);
      metricsService.getMeter("POST-".concat(tenantIdentifier)
          .concat("-blobs-success-") + responseCode).mark();
      metricsService.getMeter("POST-".concat(tenantIdentifier)
          .concat("-messages-success-") + responseCode).mark(numberOfMessages);
      metricsService.getHistogram("POST-".concat(tenantIdentifier).concat("-time-taken-ms"))
          .update(timeTaken);
    } else {
      SystemAlertUtils.updatePostToLintFailureMetric(numberOfMessages);
      metricsService.getMeter("POST-".concat(tenantIdentifier).concat("-blobs-failure-"
          + responseCode)).mark();
      metricsService.getMeter("POST-".concat(tenantIdentifier).concat("-messages-failure-")
          + responseCode).mark(numberOfMessages);
      metricsService.getHistogram("POST-".concat(tenantIdentifier).concat("failure-time-taken-ms"))
          .update(timeTaken);
    }
  }

  @Override
  public String toString() {
    return "PostToLogIqProcessor{" + "url='" + url + '\'' + ", accessKey='" + "****" + '\''
        + ", tenantIdentifier='" + tenantIdentifier + '\'' + '}';
  }
}
