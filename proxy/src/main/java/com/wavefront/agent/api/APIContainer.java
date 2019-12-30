package com.wavefront.agent.api;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.JsonNodeWriter;
import com.wavefront.agent.SSLConnectionSocketFactoryImpl;
import com.wavefront.agent.channel.DisableGZIPEncodingInterceptor;
import com.wavefront.agent.config.ProxyConfig;
import com.wavefront.api.EventAPI;
import com.wavefront.api.ProxyV2API;
import com.wavefront.api.SourceTagAPI;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.jboss.resteasy.client.jaxrs.ClientHttpEngine;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.client.jaxrs.engines.ApacheHttpClient4Engine;
import org.jboss.resteasy.client.jaxrs.internal.LocalResteasyProviderFactory;
import org.jboss.resteasy.plugins.interceptors.encoding.AcceptEncodingGZIPFilter;
import org.jboss.resteasy.plugins.interceptors.encoding.GZIPDecodingInterceptor;
import org.jboss.resteasy.plugins.interceptors.encoding.GZIPEncodingInterceptor;
import org.jboss.resteasy.plugins.providers.jackson.ResteasyJackson2Provider;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import javax.ws.rs.client.ClientRequestFilter;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.concurrent.TimeUnit;

/**
 * Container for all Wavefront back-end API objects (proxy, source tag, event)
 *
 * @author vasily@wavefront.com
 */
public class APIContainer {
  private final ProxyConfig proxyConfig;
  private final ResteasyProviderFactory resteasyProviderFactory;
  private final ClientHttpEngine clientHttpEngine;
  private ProxyV2API proxyV2API;
  private SourceTagAPI sourceTagAPI;
  private EventAPI eventAPI;

  /**
   * @param proxyConfig proxy configuration settings
   */
  public APIContainer(ProxyConfig proxyConfig) {
    this.proxyConfig = proxyConfig;
    this.resteasyProviderFactory = createProviderFactory();
    this.clientHttpEngine = createHttpEngine();
    this.proxyV2API = createService(proxyConfig.getServer(), ProxyV2API.class);
    this.sourceTagAPI = createService(proxyConfig.getServer(), SourceTagAPI.class);
    this.eventAPI = createService(proxyConfig.getServer(), EventAPI.class);
    configureHttpProxy();
  }

  /**
   * This is for testing only, as it loses ability to invoke updateServerEndpointURL().
   *
   * @param proxyV2API   RESTeasy proxy for ProxyV2API
   * @param sourceTagAPI RESTeasy proxy for SourceTagAPI
   * @param eventAPI     RESTeasy proxy for EventAPI
   */
  @VisibleForTesting
  public APIContainer(ProxyV2API proxyV2API, SourceTagAPI sourceTagAPI, EventAPI eventAPI) {
    this.proxyConfig = null;
    this.resteasyProviderFactory = null;
    this.clientHttpEngine = null;
    this.proxyV2API = proxyV2API;
    this.sourceTagAPI = sourceTagAPI;
    this.eventAPI = eventAPI;
  }

  /**
   * Get RESTeasy proxy for {@link ProxyV2API}.
   *
   * @return proxy object
   */
  public ProxyV2API getProxyV2API() {
    return proxyV2API;
  }

  /**
   * Get RESTeasy proxy for {@link SourceTagAPI}.
   *
   * @return proxy object
   */
  public SourceTagAPI getSourceTagAPI() {
    return sourceTagAPI;
  }

  /**
   * Get RESTeasy proxy for {@link EventAPI}.
   *
   * @return proxy object
   */
  public EventAPI getEventAPI() {
    return eventAPI;
  }

  /**
   * Re-create RESTeasy proxies with new server endpoint URL (allows changing URL at runtime).
   *
   * @param serverEndpointUrl new server endpoint URL.
   */
  public void updateServerEndpointURL(String serverEndpointUrl) {
    if (proxyConfig == null) {
      throw new IllegalStateException("Can't invoke updateServerEndpointURL with this constructor");
    }
    this.proxyV2API = createService(serverEndpointUrl, ProxyV2API.class);
    this.sourceTagAPI = createService(serverEndpointUrl, SourceTagAPI.class);
    this.eventAPI = createService(serverEndpointUrl, EventAPI.class);
  }

  private void configureHttpProxy() {
    if (proxyConfig.getProxyHost() != null) {
      System.setProperty("http.proxyHost", proxyConfig.getProxyHost());
      System.setProperty("https.proxyHost", proxyConfig.getProxyHost());
      System.setProperty("http.proxyPort", String.valueOf(proxyConfig.getProxyPort()));
      System.setProperty("https.proxyPort", String.valueOf(proxyConfig.getProxyPort()));
    }
    if (proxyConfig.getProxyUser() != null && proxyConfig.getProxyPassword() != null) {
      Authenticator.setDefault(
          new Authenticator() {
            @Override
            public PasswordAuthentication getPasswordAuthentication() {
              if (getRequestorType() == RequestorType.PROXY) {
                return new PasswordAuthentication(proxyConfig.getProxyUser(),
                    proxyConfig.getProxyPassword().toCharArray());
              } else {
                return null;
              }
            }
          }
      );
    }
  }

  private ResteasyProviderFactory createProviderFactory() {
    ResteasyProviderFactory factory = new LocalResteasyProviderFactory(
        ResteasyProviderFactory.getInstance());
    factory.registerProvider(JsonNodeWriter.class);
    if (!factory.getClasses().contains(ResteasyJackson2Provider.class)) {
      factory.registerProvider(ResteasyJackson2Provider.class);
    }
    return factory;
  }

  private ClientHttpEngine createHttpEngine() {
    HttpClient httpClient = HttpClientBuilder.create().
        useSystemProperties().
        setUserAgent(proxyConfig.getHttpUserAgent()).
        setMaxConnTotal(proxyConfig.getHttpMaxConnTotal()).
        setMaxConnPerRoute(proxyConfig.getHttpMaxConnPerRoute()).
        setConnectionTimeToLive(1, TimeUnit.MINUTES).
        setDefaultSocketConfig(
            SocketConfig.custom().
                setSoTimeout(proxyConfig.getHttpRequestTimeout()).build()).
        setSSLSocketFactory(new SSLConnectionSocketFactoryImpl(
            SSLConnectionSocketFactory.getSystemSocketFactory(),
            proxyConfig.getHttpRequestTimeout())).
        setRetryHandler(new DefaultHttpRequestRetryHandler(proxyConfig.getHttpAutoRetries(), true) {
          @Override
          protected boolean handleAsIdempotent(HttpRequest request) {
            // by default, retry all http calls (submissions are idempotent).
            return true;
          }
        }).
        setDefaultRequestConfig(
            RequestConfig.custom().
                setContentCompressionEnabled(true).
                setRedirectsEnabled(true).
                setConnectTimeout(proxyConfig.getHttpConnectTimeout()).
                setConnectionRequestTimeout(proxyConfig.getHttpConnectTimeout()).
                setSocketTimeout(proxyConfig.getHttpRequestTimeout()).build()).
        build();
    final ApacheHttpClient4Engine httpEngine = new ApacheHttpClient4Engine(httpClient, true);
    // avoid using disk at all
    httpEngine.setFileUploadInMemoryThresholdLimit(100);
    httpEngine.setFileUploadMemoryUnit(ApacheHttpClient4Engine.MemoryUnit.MB);
    return httpEngine;
  }

  /**
   * Create RESTeasy proxies for remote calls via HTTP.
   */
  private <T> T createService(String serverEndpointUrl, Class<T> apiClass) {
    ResteasyClient client = new ResteasyClientBuilder().
        httpEngine(clientHttpEngine).
        providerFactory(resteasyProviderFactory).
        register(GZIPDecodingInterceptor.class).
        register(proxyConfig.isGzipCompression() ?
            GZIPEncodingInterceptor.class : DisableGZIPEncodingInterceptor.class).
        register(AcceptEncodingGZIPFilter.class).
        register((ClientRequestFilter) context -> {
          if (context.getUri().getPath().contains("/v2/wfproxy") ||
              context.getUri().getPath().contains("/v2/source") ||
              context.getUri().getPath().contains("/event")) {
            context.getHeaders().add("Authorization", "Bearer " + proxyConfig.getToken());
          }
        }).
        build();
    ResteasyWebTarget target = client.target(serverEndpointUrl);
    return target.proxy(apiClass);
  }
}
