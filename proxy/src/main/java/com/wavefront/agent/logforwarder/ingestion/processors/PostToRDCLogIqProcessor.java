package com.wavefront.agent.logforwarder.ingestion.processors;

import java.util.concurrent.TimeUnit;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import com.vmware.log.forwarder.lemansclient.LemansClientUtils;
import com.wavefront.agent.logforwarder.config.LogForwarderConfigProperties;
import com.vmware.xenon.common.UriUtils;
import com.wavefront.agent.logforwarder.constants.LogForwarderConstants;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.IngestionClient;
import com.wavefront.agent.logforwarder.ingestion.http.client.utils.HttpClientUtils;
import com.wavefront.agent.logforwarder.ingestion.util.LogForwarderUtils;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/2/21 4:14 PM
 */
public class PostToRDCLogIqProcessor extends PostToLogIqProcessor implements Processor {

  /**
   *
   * @param json
   * @throws Throwable
   */
  @Override
  public void initializeProcessor(JSONAware json) throws Throwable {
    JSONObject jsonObject = (JSONObject) json;

    if (jsonObject.containsKey(LogForwarderConstants.CHAIN_NAME)) {
      chainName = jsonObject.get(LogForwarderConstants.CHAIN_NAME).toString();
    }

    url = LogForwarderConfigProperties.logForwarderArgs.lemansServerUrl;//TODO Move this to
    // proxyConfig
    accessKey = LogForwarderUtils.getLemansClientAccessKey();//TODO move this to ProxyConfig
    tenantIdentifier = LogForwarderConstants.RDC_TENANT_IDENTIFIER;
    try {
      LemansClientUtils.getInstance().initializeVertxLemansClient(url, accessKey);
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
