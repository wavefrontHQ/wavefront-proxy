package com.wavefront.agent.logforwarder.ingestion.processors;

import java.util.concurrent.TimeUnit;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.wavefront.agent.logforwarder.constants.LogForwarderConstants;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.GatewayClientFactory;
import com.wavefront.agent.logforwarder.ingestion.http.client.utils.HttpClientUtils;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;
import com.wavefront.agent.logforwarder.ingestion.util.UriUtils;

/**
 * Processor
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/2/21 4:14 PM
 */
public class PostToRDCLogIqProcessor extends PostToLogIqProcessor implements Processor {

  /**
   * Initialize the processor with configuration from xml
   * @param json
   * @throws Throwable
   */
  @Override
  public void initializeProcessor(JSONAware json) throws Throwable {
    JSONObject jsonObject = (JSONObject) json;

    if (jsonObject.containsKey(LogForwarderConstants.CHAIN_NAME)) {
      chainName = jsonObject.get(LogForwarderConstants.CHAIN_NAME).toString();
    }

//    url = LogForwarderConfigProperties.logForwarderArgs.lemansServerUrl;//TODO Move this to
    // proxyConfig
    url = jsonObject.get(LogForwarderConstants.INGESTION_GATEWAY_URL).toString();

//    accessKey = LogForwarderUtils.getLemansClientAccessKey();//TODO move this to ProxyConfig
    accessKey =  jsonObject.get(LogForwarderConstants.INGESTION_GATEWAY_ACCESS_TOKEN).toString();
    tenantIdentifier = LogForwarderConstants.RDC_TENANT_IDENTIFIER;
    bufferDiskLocation = jsonObject.get(LogForwarderConstants.INGESTION_DISK_QUEUE_LOCATION).toString();
    try {
      GatewayClientFactory.getInstance().initializeVertxLemansClient(url, accessKey, bufferDiskLocation);
    } catch (RuntimeException e) {
      throw e;
    }
    streamUri = UriUtils.extendUri(UriUtils.buildUri(url), "le-mans/v1/streams/ingestion-pipeline" +
        "-stream");//TODO move this to some other generic URI helper not xenon
    HttpClientUtils
        .createAsyncHttpClient(accessKey, (int) TimeUnit.SECONDS.toMillis(httpTimeOutSecs), Boolean.TRUE);
    httpAsyncClient = HttpClientUtils.getHttpClient(accessKey);

  }

  @Override
  public String toString() {
    return "PostToRDCLogIqProcessor{" + "url='" + url + '\'' + ", accessKey='" + "****" + '\''
        + ", tenantIdentifier='" + tenantIdentifier + '\'' + '}';
  }
}
