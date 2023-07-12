package com.wavefront.agent.api;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.wavefront.agent.*;
import com.wavefront.agent.channel.DisableGZIPEncodingInterceptor;
import com.wavefront.agent.channel.GZIPEncodingInterceptorWithVariableCompression;
import com.wavefront.api.EventAPI;
import com.wavefront.api.LogAPI;
import com.wavefront.api.ProxyV2API;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.common.LeMansAPI;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.ext.WriterInterceptor;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.resteasy.client.jaxrs.ClientHttpEngine;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.client.jaxrs.engines.ApacheHttpClient43Engine;
import org.jboss.resteasy.client.jaxrs.internal.LocalResteasyProviderFactory;
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl;
import org.jboss.resteasy.plugins.interceptors.AcceptEncodingGZIPFilter;
import org.jboss.resteasy.plugins.interceptors.GZIPDecodingInterceptor;
import org.jboss.resteasy.plugins.providers.jackson.ResteasyJackson2Provider;
import org.jboss.resteasy.spi.ResteasyProviderFactory;

/**
 * Container for all Wavefront back-end API objects (proxy, source tag, event)
 *
 * @author vasily@wavefront.com
 */
public class APIContainer {
  public static final String CENTRAL_TENANT_NAME = "central";
  public static final String API_SERVER = "server";
  public static final String LEMANS_SERVER = "le-mans-server";
  public static final String API_TOKEN = "token";
  public static final String LE_MANS_INGESTION_PATH =
      "le-mans/v1/streams/ingestion-pipeline-stream";

  private final ProxyConfig proxyConfig;
  private final ResteasyProviderFactory resteasyProviderFactory;
  private final ClientHttpEngine clientHttpEngine;
  private final boolean discardData;
  private final CSPAPI cspAPI;

  private Map<String, ProxyV2API> proxyV2APIsForMulticasting;
  private Map<String, SourceTagAPI> sourceTagAPIsForMulticasting;
  private Map<String, EventAPI> eventAPIsForMulticasting;
  private Map<String, LeMansAPI> leMansAPIsForMulticasting;

  private LogAPI logAPI;

  private String logServerToken;
  private String logServerEndpointUrl;

  private static final Logger logger = LogManager.getLogger(APIContainer.class.getCanonicalName());

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
    this.logAPI = createService(logServerEndpointUrl, LogAPI.class);
    this.cspAPI = createService(proxyConfig.getCSPBaseUrl(), CSPAPI.class);

    // config the multicasting tenants / clusters
    proxyV2APIsForMulticasting = Maps.newHashMap();
    sourceTagAPIsForMulticasting = Maps.newHashMap();
    eventAPIsForMulticasting = Maps.newHashMap();
    leMansAPIsForMulticasting = Maps.newHashMap();
    // tenantInfo: {<tenant_name> : {"token": <wf_token>, "server": <wf_sever_url>}}
    String tenantName;
    String tenantServer;
    String leMansServer;
    for (Map.Entry<String, TenantInfo> tenantInfoEntry :
        TokenManager.getMulticastingTenantList().entrySet()) {
      tenantName = tenantInfoEntry.getKey();
      tenantServer = tenantInfoEntry.getValue().getWFServer();
      proxyV2APIsForMulticasting.put(tenantName, createService(tenantServer, ProxyV2API.class));
      sourceTagAPIsForMulticasting.put(tenantName, createService(tenantServer, SourceTagAPI.class));
      eventAPIsForMulticasting.put(tenantName, createService(tenantServer, EventAPI.class));
      leMansAPIsForMulticasting.put(tenantName, createService(tenantServer, LeMansAPI.class));
    }

    if (discardData) {
      ProxyV2API proxyV2API = this.proxyV2APIsForMulticasting.get(CENTRAL_TENANT_NAME);
      this.proxyV2APIsForMulticasting = Maps.newHashMap();
      this.proxyV2APIsForMulticasting.put(CENTRAL_TENANT_NAME, new NoopProxyV2API(proxyV2API));
      this.sourceTagAPIsForMulticasting = Maps.newHashMap();
      this.sourceTagAPIsForMulticasting.put(CENTRAL_TENANT_NAME, new NoopSourceTagAPI());
      this.eventAPIsForMulticasting = Maps.newHashMap();
      this.eventAPIsForMulticasting.put(CENTRAL_TENANT_NAME, new NoopEventAPI());
      this.leMansAPIsForMulticasting = Maps.newHashMap();
      this.logAPI = new NoopLogAPI();
    }
    configureHttpProxy();
  }

  /**
   * This is for testing only, as it loses ability to invoke updateServerEndpointURL().
   *
   * @param proxyV2API RESTeasy proxy for ProxyV2API
   * @param sourceTagAPI RESTeasy proxy for SourceTagAPI
   * @param eventAPI RESTeasy proxy for EventAPI
   * @param logAPI RESTeasy proxy for LogAPI
   */
  @VisibleForTesting
  public APIContainer(
      ProxyV2API proxyV2API,
      SourceTagAPI sourceTagAPI,
      EventAPI eventAPI,
      LogAPI logAPI,
      LeMansAPI leMansAPI,
      CSPAPI cspAPI) {
    this.proxyConfig = null;
    this.resteasyProviderFactory = null;
    this.clientHttpEngine = null;
    this.discardData = false;
    this.logAPI = logAPI;
    this.cspAPI = cspAPI;
    proxyV2APIsForMulticasting = Maps.newHashMap();
    proxyV2APIsForMulticasting.put(CENTRAL_TENANT_NAME, proxyV2API);
    sourceTagAPIsForMulticasting = Maps.newHashMap();
    sourceTagAPIsForMulticasting.put(CENTRAL_TENANT_NAME, sourceTagAPI);
    eventAPIsForMulticasting = Maps.newHashMap();
    eventAPIsForMulticasting.put(CENTRAL_TENANT_NAME, eventAPI);
    leMansAPIsForMulticasting = Maps.newHashMap();
    leMansAPIsForMulticasting.put(CENTRAL_TENANT_NAME, leMansAPI);
  }

  /**
   * Provide the collection of loaded tenant name list
   *
   * @return tenant name collection
   */
  public Collection<String> getTenantNameList() {
    return proxyV2APIsForMulticasting.keySet();
  }

  /**
   * Get RESTeasy proxy for {@link ProxyV2API} with given tenant name.
   *
   * @param tenantName tenant name
   * @return proxy object corresponding to tenant
   */
  public ProxyV2API getProxyV2APIForTenant(String tenantName) {
    return proxyV2APIsForMulticasting.get(tenantName);
  }

  /**
   * Get RESTeasy proxy for {@link ProxyV2API} with given tenant name.
   *
   * @param tenantName tenant name
   * @return proxy object corresponding to tenant
   */
  public LeMansAPI getLeMansAPIForTenant(String tenantName) {
    return leMansAPIsForMulticasting.get(tenantName);
  }

  /**
   * Get RESTeasy proxy for {@link SourceTagAPI} with given tenant name.
   *
   * @param tenantName tenant name
   * @return proxy object corresponding to the tenant name
   */
  public SourceTagAPI getSourceTagAPIForTenant(String tenantName) {
    return sourceTagAPIsForMulticasting.get(tenantName);
  }

  /**
   * Get RESTeasy proxy for {@link EventAPI} with given tenant name.
   *
   * @param tenantName tenant name
   * @return proxy object corresponding to the tenant name
   */
  public EventAPI getEventAPIForTenant(String tenantName) {
    return eventAPIsForMulticasting.get(tenantName);
  }

  /**
   * Get RESTeasy proxy for {@link LogAPI}.
   *
   * @return proxy object
   */
  public LogAPI getLogAPI() {
    return logAPI;
  }

  public String getLogServerToken() {
    return logServerToken;
  }

  public String getLogServerEndpointUrl() {
    return logServerEndpointUrl;
  }

  /**
   * Re-create RESTeasy proxies with new server endpoint URL (allows changing URL at runtime).
   *
   * @param logServerEndpointUrl new log server endpoint URL.
   * @param logServerToken new server token.
   */
  public void updateLogServerEndpointURLandToken(
      String logServerEndpointUrl, String logServerToken) {
    // if either are null or empty, just return
    if (StringUtils.isBlank(logServerEndpointUrl) || StringUtils.isBlank(logServerToken)) {
      return;
    }
    // Only recreate if either the url or token have changed
    if (!StringUtils.equals(logServerEndpointUrl, this.logServerEndpointUrl)
        || !StringUtils.equals(logServerToken, this.logServerToken)) {
      this.logServerEndpointUrl = removePathFromURL(logServerEndpointUrl);
      this.logServerToken = logServerToken;
      this.logAPI = createService(this.logServerEndpointUrl, LogAPI.class, createProviderFactory());
      if (discardData) {
        this.logAPI = new NoopLogAPI();
      }
    }
  }

  private String removePathFromURL(String completeURL) {
    if (completeURL != null && completeURL.contains(LE_MANS_INGESTION_PATH)) {
      return completeURL.replace(LE_MANS_INGESTION_PATH, "");
    }
    return completeURL;
  }

  /**
   * Re-create RESTeasy proxies with new server endpoint URL (allows changing URL at runtime).
   *
   * @param tenantName the tenant to be updated
   * @param serverEndpointUrl new server endpoint URL.
   */
  public void updateServerEndpointURL(String tenantName, String serverEndpointUrl) {
    if (proxyConfig == null) {
      throw new IllegalStateException("Can't invoke updateServerEndpointURL with this constructor");
    }
    proxyV2APIsForMulticasting.put(tenantName, createService(serverEndpointUrl, ProxyV2API.class));
    sourceTagAPIsForMulticasting.put(
        tenantName, createService(serverEndpointUrl, SourceTagAPI.class));
    eventAPIsForMulticasting.put(tenantName, createService(serverEndpointUrl, EventAPI.class));

    if (discardData) {
      ProxyV2API proxyV2API = this.proxyV2APIsForMulticasting.get(CENTRAL_TENANT_NAME);
      this.proxyV2APIsForMulticasting = Maps.newHashMap();
      this.proxyV2APIsForMulticasting.put(CENTRAL_TENANT_NAME, new NoopProxyV2API(proxyV2API));
      this.sourceTagAPIsForMulticasting = Maps.newHashMap();
      this.sourceTagAPIsForMulticasting.put(CENTRAL_TENANT_NAME, new NoopSourceTagAPI());
      this.eventAPIsForMulticasting = Maps.newHashMap();
      this.eventAPIsForMulticasting.put(CENTRAL_TENANT_NAME, new NoopEventAPI());
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
                return new PasswordAuthentication(
                    proxyConfig.getProxyUser(), proxyConfig.getProxyPassword().toCharArray());
              } else {
                return null;
              }
            }
          });
    }
  }

  private ResteasyProviderFactory createProviderFactory() {
    ResteasyProviderFactory factory =
        new LocalResteasyProviderFactory(ResteasyProviderFactory.getInstance());
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
    // passed as a parameter, it's creating duplicate headers that cause the entire request to
    // be
    // rejected by nginx. unfortunately, RESTeasy is not smart enough to handle that
    // automatically.
    factory.register(
        (ClientRequestFilter)
            context -> {
              if (context.getUri().getPath().startsWith("/csp")) {
                return;
              }
              if (proxyConfig.isGzipCompression()) {
                context.getHeaders().add("Content-Encoding", "gzip");
              }
              if ((context.getUri().getPath().contains("/v2/wfproxy")
                      || context.getUri().getPath().contains("/v2/source")
                      || context.getUri().getPath().contains("/event"))
                  && !context.getUri().getPath().endsWith("checkin")) {
                context
                    .getHeaders()
                    .add(
                        "Authorization",
                        "Bearer "
                            + TokenManager.getMulticastingTenantList()
                                .get(APIContainer.CENTRAL_TENANT_NAME)
                                .getBearerToken());
              } else if (context.getUri().getPath().contains("/le-mans")) {
                context.getHeaders().add("Authorization", "Bearer " + proxyConfig.getLeMansToken());
              }
            });
    return factory;
  }

  private ClientHttpEngine createHttpEngine() {
    HttpClient httpClient =
        HttpClientBuilder.create()
            .useSystemProperties()
            .setUserAgent(proxyConfig.getHttpUserAgent())
            .setMaxConnTotal(proxyConfig.getHttpMaxConnTotal())
            .setMaxConnPerRoute(proxyConfig.getHttpMaxConnPerRoute())
            .setConnectionTimeToLive(1, TimeUnit.MINUTES)
            .setDefaultSocketConfig(
                SocketConfig.custom().setSoTimeout(proxyConfig.getHttpRequestTimeout()).build())
            .setSSLSocketFactory(
                new SSLConnectionSocketFactoryImpl(
                    SSLConnectionSocketFactory.getSystemSocketFactory(),
                    proxyConfig.getHttpRequestTimeout()))
            .setRetryHandler(
                new DefaultHttpRequestRetryHandler(proxyConfig.getHttpAutoRetries(), true) {
                  @Override
                  protected boolean handleAsIdempotent(HttpRequest request) {
                    // by default, retry all http calls (submissions are
                    // idempotent).
                    return true;
                  }
                })
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setContentCompressionEnabled(true)
                    .setRedirectsEnabled(true)
                    .setConnectTimeout(proxyConfig.getHttpConnectTimeout())
                    .setConnectionRequestTimeout(proxyConfig.getHttpConnectTimeout())
                    .setSocketTimeout(proxyConfig.getHttpRequestTimeout())
                    .build())
            .build();
    final ApacheHttpClient43Engine httpEngine = new ApacheHttpClient43Engine(httpClient, true);
    // avoid using disk at all
    httpEngine.setFileUploadInMemoryThresholdLimit(100);
    httpEngine.setFileUploadMemoryUnit(ApacheHttpClient43Engine.MemoryUnit.MB);
    return httpEngine;
  }

  /** Create RESTeasy proxies for remote calls via HTTP. */
  private <T> T createService(String serverEndpointUrl, Class<T> apiClass) {
    return createServiceInternal(serverEndpointUrl, apiClass, resteasyProviderFactory);
  }

  /** Create RESTeasy proxies for remote calls via HTTP. */
  private <T> T createService(
      String serverEndpointUrl,
      Class<T> apiClass,
      ResteasyProviderFactory resteasyProviderFactory) {
    return createServiceInternal(serverEndpointUrl, apiClass, resteasyProviderFactory);
  }

  /** Create RESTeasy proxies for remote calls via HTTP. */
  private <T> T createServiceInternal(
      String serverEndpointUrl,
      Class<T> apiClass,
      ResteasyProviderFactory resteasyProviderFactory) {
    ResteasyClient client =
        new ResteasyClientBuilderImpl()
            .httpEngine(clientHttpEngine)
            .providerFactory(resteasyProviderFactory)
            .build();
    ResteasyWebTarget target = client.target(serverEndpointUrl);
    return target.proxy(apiClass);
  }

  public String getLeMansStreamName() {
    try {
      return proxyConfig.getLeMansStreamName();
    } catch (NullPointerException e) {
      return null;
    }
  }

  public CSPAPI getCSPApi() {
    return cspAPI;
  }
}
