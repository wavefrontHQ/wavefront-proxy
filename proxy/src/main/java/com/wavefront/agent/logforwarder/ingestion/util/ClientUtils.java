package com.wavefront.agent.logforwarder.ingestion.util;


import com.esotericsoftware.kryo.io.Output;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.constants.GatewayConstants;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.GatewayOperation;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.ServiceErrorResponse;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization.KryoSerializers;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.Utils;
import com.wavefront.agent.logforwarder.ingestion.http.client.ProxyConfiguration;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.impl.headers.VertxHttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.core.net.ProxyOptions;
import io.vertx.core.net.ProxyType;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 4:26 PM
 */
public class ClientUtils {
    private static final Logger logger = LoggerFactory.getLogger(ClientUtils.class);
  private static Set<String> CONNECTION_HEADERS = new HashSet(Arrays.asList("content-length", "connection", "content-type"));


  private ClientUtils() {
  }

  public static MultiMap convertToMultiMap(Map<String, String> headers) {
    MultiMap multiMap = new VertxHttpHeaders();
    if (headers != null && !headers.isEmpty()) {
      multiMap.addAll(headers);
      return multiMap;
    } else {
      return multiMap;
    }
  }

  public static boolean handleServerErrorResponse(Buffer serverSentEventBuffer) {
    try {
      ServiceErrorResponse serviceErrorResponse =
          Json.decodeValue(serverSentEventBuffer, ServiceErrorResponse.class);
      boolean error = serviceErrorResponse.message != null || serviceErrorResponse.statusCode > 0;
      if (error) {
        logger.error("Lemans gateway returned error: StatusCode: {}, Message: {}",
            serviceErrorResponse.statusCode, serviceErrorResponse.message);
      }
      return error;
    } catch (Exception e) {
      return false;
    }
  }

  public static void setNetworkProxy(HttpClientOptions httpClientOptions, ProxyConfiguration proxyConfig,
                                     String serviceName) {
    if (StringUtils.isNotEmpty(proxyConfig.getHost())) {
      logger.info("Configuring the {} client to work with proxy Host: {}, Port: {}",
          serviceName, proxyConfig.getHost(), proxyConfig.getPort());
      ProxyOptions proxyOptions = new ProxyOptions();
      proxyOptions.setType(ProxyType.HTTP);
      proxyOptions.setHost(proxyConfig.getHost());
      proxyOptions.setPort(proxyConfig.getPort());
      if (StringUtils.isNotEmpty(proxyConfig.getUser())) {
        proxyOptions.setUsername(proxyConfig.getUser());
        proxyOptions.setPassword(proxyConfig.getPassword());
      }
      httpClientOptions.setProxyOptions(proxyOptions);
    } else {
      logger.info("Proxy is not configured for {} client.", serviceName);
    }
  }

  public static HttpMethod httpMethod(String action) {
    return HttpMethod.valueOf(action);
  }

  public static Map<String, String> getRequestHeaders(MultiMap headers) {
    Map<String, String> map = new HashMap();
    if (headers != null && !headers.isEmpty()) {
      Iterator var2 = headers.entries().iterator();

      while (var2.hasNext()) {
        Map.Entry<String, String> entry = (Map.Entry) var2.next();
        if (!CONNECTION_HEADERS.contains(entry.getKey())) {
          map.put(entry.getKey(), entry.getValue());
        }
      }

      return map;
    } else {
      return map;
    }
  }

  public static Pair<byte[], String> encode(Object body, String contentType, String action, String encoding) {
    String newContentType = contentType;
    if (body == null) {
      return new ImmutablePair<>(Buffer.buffer().getBytes(), contentType);
    }
    byte[] data = null;
    if (body instanceof String) {
      data = ((String) body).getBytes(StandardCharsets.UTF_8);
    } else if (body instanceof byte[]) {
      data = (byte[]) body;
      newContentType = GatewayConstants.MEDIA_TYPE_APPLICATION_OCTET_STREAM;
    } else if (GatewayConstants.MEDIA_TYPE_APPLICATION_KRYO_OCTET_STREAM.equals(contentType)) {
      Output o = KryoSerializers.serializeAsDocument(body, GatewayConstants.MAX_BINARY_SERIALIZED_BODY_LIMIT);
      data = o.toBytes();
    }

    if (data == null) {
      String encodedBody = Utils.toJson(body);
      data = encodedBody.getBytes(StandardCharsets.UTF_8);
      if (!action.equalsIgnoreCase(GatewayOperation.Action.GET.toString()) && contentType == null) {
        newContentType = GatewayConstants.MEDIA_TYPE_APPLICATION_JSON;
      }
    }

    boolean gzip = GatewayConstants.GZIP.equals(encoding);
    if (gzip) {
      data = Utils.compressToGzip(data);
    }
    return new ImmutablePair<>(data, newContentType);
  }

  public static Object decode(Buffer buffer, String contentType) {
    if (buffer == null) {
      return null;
    }
    Object body = decodeIfText(buffer, contentType);
    if (body != null) {
      return body;
    } else {
      return buffer.getBytes();
    }
  }

  private static String decodeIfText(Buffer buffer, String contentType) {
    if (contentType == null) {
      return null;
    } else {
      String body = null;
      if (Utils.isContentTypeText(contentType)) {
        body = buffer.toString();
      } else if (contentType.contains(GatewayConstants.MEDIA_TYPE_APPLICATION_X_WWW_FORM_ENCODED)) {
        body = buffer.toString();

        try {
          body = URLDecoder.decode(body, GatewayConstants.UTF8);
        } catch (UnsupportedEncodingException var5) {
          throw new RuntimeException(var5);
        }
      }

      return body;
    }
  }
}
