package com.wavefront.agent.logforwarder.ingestion.utils;

import com.wavefront.agent.logforwarder.constants.LogForwarderConstants;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.metrics.MetricsService;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.FNVHash;
import com.wavefront.agent.logforwarder.ingestion.util.RestApiSourcesMetricsUtil;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import io.dropwizard.metrics5.MetricFilter;
import io.vertx.core.http.HttpMethod;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 10/4/21 10:04 AM
 */
public class TestUtils {
  public static void clearMetrics() {
    MetricsService.getInstance().primaryRegistry.removeMatching(MetricFilter.ALL);
//    SystemAlertUtils.systemAlertMetricRegistry.removeMatching(MetricFilter.ALL);//TODO Revist this
    RestApiSourcesMetricsUtil.restApiMetricsRegistry.removeMatching(MetricFilter.ALL);
    RestApiSourcesMetricsUtil.structureVsSources.clear();
    System.clearProperty(LogForwarderConstants.DISABLE_HTTP2_PROPERTY);
  }

  public static String readFile(ClassLoader classLoader, String fileName) throws Exception {
    System.out.println("Reading file name: " +fileName);
    return IOUtils.toString(classLoader.getResourceAsStream(fileName), StandardCharsets.UTF_8);
  }

  public static HttpUriRequest createHttpRequest(String url, Map<String, String> headers, String body,
                                                 HttpMethod reqType) throws Exception {
    HttpUriRequest request = null;
    if (reqType.equals(HttpMethod.POST)) {
      request = new HttpPost(url);
      ((HttpPost) request).setEntity(new StringEntity(body));
    } else if (reqType.equals(HttpMethod.GET)) {
      request = new HttpGet(url);
    } else {
      throw new RuntimeException("Unsupported http request type");
    }
    if (MapUtils.isNotEmpty(headers)) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        request.addHeader(header.getKey(), header.getValue());
      }
    }
    return request;
  }

  public static String computeHash(CharSequence content) {
    return Long.toHexString(FNVHash.compute(content));
  }
}
