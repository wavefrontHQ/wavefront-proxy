package com.wavefront.agent.logforwarder.ingestion.http.client;



import com.wavefront.agent.logforwarder.ingestion.util.ClientUtils;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 3:57 PM
 */
public class HttpClientFactory {
  private HttpClientFactory() {
  }

  public static WebClient createWebClient(HttpClientFactory.Builder builder) {
    if (builder.vertx == null) {
      throw new IllegalArgumentException("Vertx can't be null");
    } else {
      WebClientOptions webClientOptions = new WebClientOptions();
      setHttpClientOptions(builder, webClientOptions);
      return WebClient.create(builder.vertx, webClientOptions);
    }
  }

  public static HttpClient createHttpClient(HttpClientFactory.Builder builder) {
    if (builder.vertx == null) {
      throw new IllegalArgumentException("Vertx can't be null");
    } else {
      HttpClientOptions httpClientOptions = new HttpClientOptions();
      setHttpClientOptions(builder, httpClientOptions);
      return builder.vertx.createHttpClient(httpClientOptions);
    }
  }

  private static void setHttpClientOptions(HttpClientFactory.Builder builder, HttpClientOptions httpClientOptions) {
    if (Boolean.TRUE.equals(builder.useNetworkProxy)) {
      ProxyConfiguration proxyConfig = ProxyConfiguration.defaultConfig();
      ClientUtils.setNetworkProxy(httpClientOptions, proxyConfig, HttpClientFactory.class.toString());
    }

    if (builder.maxPoolSize != null) {
      httpClientOptions.setMaxPoolSize(builder.maxPoolSize);
    }

    if (builder.connTimeoutMillis != null) {
      httpClientOptions.setConnectTimeout(builder.connTimeoutMillis);
    }

    if (builder.maxChunkSize != null) {
      httpClientOptions.setMaxChunkSize(builder.maxChunkSize);
    }

    if (builder.ssl != null && Boolean.TRUE.equals(builder.ssl)) {
      httpClientOptions.setSsl(builder.ssl);
    }

  }

  public static class Builder {
    private Vertx vertx;
    private Integer maxPoolSize;
    private Integer connTimeoutMillis;
    private Boolean useNetworkProxy;
    private Integer maxChunkSize;
    private Boolean ssl;

    public Builder() {
    }

    public HttpClientFactory.Builder vertx(Vertx vertx) {
      this.vertx = vertx;
      return this;
    }

    public HttpClientFactory.Builder maxPoolSize(int maxPoolSize) {
      this.maxPoolSize = maxPoolSize;
      return this;
    }

    public HttpClientFactory.Builder connTimeoutMillis(int connTimeoutMillis) {
      this.connTimeoutMillis = connTimeoutMillis;
      return this;
    }

    public HttpClientFactory.Builder useNetworkProxy(boolean useNetworkProxy) {
      this.useNetworkProxy = useNetworkProxy;
      return this;
    }

    public HttpClientFactory.Builder maxChunkSize(int maxChunkSize) {
      this.maxChunkSize = maxChunkSize;
      return this;
    }

    public HttpClientFactory.Builder ssl(boolean ssl) {
      this.ssl = ssl;
      return this;
    }
  }
}
