package com.wavefront.agent.api;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.wavefront.agent.JsonNodeWriter;
import com.wavefront.agent.SSLConnectionSocketFactoryImpl;
import com.wavefront.agent.channel.DisableGZIPEncodingInterceptor;
import com.wavefront.agent.channel.GZIPEncodingInterceptorWithVariableCompression;
import com.wavefront.agent.ProxyConfig;
import com.wavefront.api.EventAPI;
import com.wavefront.api.LogAPI;
import com.wavefront.api.ProxyV2API;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.dto.Log;

import org.apache.commons.lang.StringUtils;
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
import org.jboss.resteasy.plugins.providers.jackson.ResteasyJackson2Provider;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.ext.WriterInterceptor;
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
  private final boolean discardData;
  private ProxyV2API proxyV2API;
  private SourceTagAPI sourceTagAPI;
  private EventAPI eventAPI;
  private LogAPI logAPI;
  private String logServerToken;
  private String logServerEndpointUrl;

  /**
   * @param proxyConfig proxy configuration settings
   * @param discardData run proxy in test mode (don't actually send the data)
   */
  public APIContainer(ProxyConfig proxyConfig, boolean discardData) {
    this.proxyConfig = proxyConfig;
    this.logServerToken = "NOT_SET";
    this.logServerEndpointUrl = "NOT_SET";
    this.resteasyProviderFactory = createProviderFactory();
    this.clientHttpEngine = createHttpEngine();
    this.discardData = discardData;
    this.proxyV2API = createService(proxyConfig.getServer(), ProxyV2API.class);
    this.sourceTagAPI = createService(proxyConfig.getServer(), SourceTagAPI.class);
    this.eventAPI = createService(proxyConfig.getServer(), EventAPI.class);
    this.logAPI = createService(logServerEndpointUrl, LogAPI.class);
    if (discardData) {
      this.proxyV2API = new NoopProxyV2API(proxyV2API);
      this.sourceTagAPI = new NoopSourceTagAPI();
      this.eventAPI = new NoopEventAPI();
      this.logAPI = new NoopLogAPI();
    }
    configureHttpProxy();
  }

  /**
   * This is for testing only, as it loses ability to invoke updateServerEndpointURL().
   *
   * @param proxyV2API   RESTeasy proxy for ProxyV2API
   * @param sourceTagAPI RESTeasy proxy for SourceTagAPI
   * @param eventAPI     RESTeasy proxy for EventAPI
   * @param logAPI       RESTeasy proxy for LogAPI
   */
  @VisibleForTesting
  public APIContainer(ProxyV2API proxyV2API, SourceTagAPI sourceTagAPI, EventAPI eventAPI, LogAPI logAPI) {
    this.proxyConfig = null;
    this.resteasyProviderFactory = null;
    this.clientHttpEngine = null;
    this.discardData = false;
    this.proxyV2API = proxyV2API;
    this.sourceTagAPI = sourceTagAPI;
    this.eventAPI = eventAPI;
    this.logAPI = logAPI;
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
   * Get RESTeasy proxy for {@link LogAPI}.
   *
   * @return proxy object
   */
  public LogAPI getLogAPI() {
    return logAPI;
  }

  /**
   * Re-create RESTeasy proxies with new server endpoint URL (allows changing URL at runtime).
   *
   * @param logServerEndpointUrl new log server endpoint URL.
   * @param logServerToken new server token.
   */
  public void updateLogServerEndpointURLandToken(String logServerEndpointUrl, String logServerToken) {
    // if either are null or empty, just return
    if (StringUtils.isBlank(logServerEndpointUrl) || StringUtils.isBlank(logServerToken)) {
      return;
    }
    // Only recreate if either the url or token have changed
    if (!StringUtils.equals(logServerEndpointUrl, this.logServerEndpointUrl) ||
        !StringUtils.equals(logServerToken, this.logServerToken)) {
      this.logServerEndpointUrl = logServerEndpointUrl;
      this.logServerToken = logServerToken;
      this.logAPI = createService(logServerEndpointUrl, LogAPI.class, createProviderFactory());
      if (discardData) {
        this.logAPI = new NoopLogAPI();
      }
    }
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
    if (discardData) {
      this.proxyV2API = new NoopProxyV2API(proxyV2API);
      this.sourceTagAPI = new NoopSourceTagAPI();
      this.eventAPI = new NoopEventAPI();
    }
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
    factory.register(GZIPDecodingInterceptor.class);
    if (proxyConfig.isGzipCompression()) {
      WriterInterceptor interceptor =
          new GZIPEncodingInterceptorWithVariableCompression(proxyConfig.getGzipCompressionLevel());
      factory.register(interceptor);
    } else {
      factory.register(DisableGZIPEncodingInterceptor.class);
    }
    factory.register(AcceptEncodingGZIPFilter.class);
    // add authorization header for all proxy endpoints, except for /checkin - since it's also
    // passed as a parameter, it's creating duplicate headers that cause the entire request to be
    // rejected by nginx. unfortunately, RESTeasy is not smart enough to handle that automatically.
    factory.register((ClientRequestFilter) context -> {
          if ((context.getUri().getPath().contains("/v2/wfproxy") ||
              context.getUri().getPath().contains("/v2/source") ||
              context.getUri().getPath().contains("/event")) &&
              !context.getUri().getPath().endsWith("checkin")) {
            context.getHeaders().add("Authorization", "Bearer " + proxyConfig.getToken());
          } else if (context.getUri().getPath().contains("/le-mans")) {
            context.getHeaders().add("Authorization", "Bearer " + logServerToken);
          }
        });
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
    return createServiceInternal(serverEndpointUrl, apiClass, resteasyProviderFactory);
  }

  /**
   * Create RESTeasy proxies for remote calls via HTTP.
   */
  private <T> T createService(String serverEndpointUrl, Class<T> apiClass, ResteasyProviderFactory resteasyProviderFactory) {
    return createServiceInternal(serverEndpointUrl, apiClass, resteasyProviderFactory);
  }

  /**
   * Create RESTeasy proxies for remote calls via HTTP.
   */
  private <T> T createServiceInternal(String serverEndpointUrl, Class<T> apiClass, ResteasyProviderFactory resteasyProviderFactory) {
    ResteasyClient client = new ResteasyClientBuilder().
        httpEngine(clientHttpEngine).
        providerFactory(resteasyProviderFactory).
        build();
    ResteasyWebTarget target = client.target(serverEndpointUrl);
    return target.proxy(apiClass);
  }
}
