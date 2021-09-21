package com.wavefront.agent.logforwarder.ingestion.http.client;

import com.wavefront.agent.logforwarder.ingestion.util.UriUtils;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 4:18 PM
 */
public class ProxyConfiguration {
  public static final String PROPERTY_PROXY_SERVER = "http_proxy_server";
  public static final String PROPERTY_PROXY_PORT = "http_proxy_port";
  public static final String PROPERTY_PROXY_USERNAME = "http_proxy_username";
  public static final String PROPERTY_PROXY_PASSWORD = "http_proxy_password";
  public static final String PROPERTY_HTTPS_PROXY = "https_proxy";
  public static final String PROPERTY_HTTP_PROXY = "http_proxy";
  public static final String PROPERTY_HTTP_PROXY_HOST_LIST = "http_proxy_host_list";
  public static final String PROPERTY_NAME_TEST_HTTP_PROXY = "lemans.test.httpProxy";
  private String host;
  private int port;
  private String user;
  private String password;
  private String hostList;
  private Set<String> hosts = new HashSet();

  public ProxyConfiguration() {
  }

  public static ProxyConfiguration defaultConfig() {
    return !StringUtils.isNotEmpty(System.getenv("http_proxy_server")) && !StringUtils.isNotEmpty(System.getenv("https_proxy")) && !StringUtils.isNotEmpty(System.getenv("http_proxy")) ? defaultConfig(System.getProperty("http_proxy_server"), System.getProperty("http_proxy_port"), System.getProperty("http_proxy_username"), System.getProperty("http_proxy_password"), System.getProperty("https_proxy"), System.getProperty("http_proxy"), System.getProperty("http_proxy_host_list")) : defaultConfig(System.getenv("http_proxy_server"), System.getenv("http_proxy_port"), System.getenv("http_proxy_username"), System.getenv("http_proxy_password"), System.getenv("https_proxy"), System.getenv("http_proxy"), System.getenv("http_proxy_host_list"));
  }

  static ProxyConfiguration defaultConfig(String proxyHost, String proxyPort, String proxyUsername, String proxyPassword, String httpsProxy, String httpProxy, String hostList) {
    ProxyConfiguration defaultConfig = new ProxyConfiguration();
    defaultConfig.hostList = hostList;
    defaultConfig.hosts.addAll(parseHostList(defaultConfig.hostList));
    defaultConfig.host = proxyHost;
    if (defaultConfig.host != null) {
      defaultConfig.port = proxyPort != null && !proxyPort.isEmpty() ? Integer.parseInt(proxyPort) : 0;
      defaultConfig.user = proxyUsername;
      defaultConfig.password = proxyPassword;
      return defaultConfig;
    } else {
      String proxyUrl = httpsProxy != null && !httpsProxy.isEmpty() ? httpsProxy : httpProxy;
      String testHttpProxy = System.getProperty("lemans.test.httpProxy");
      if ((proxyUrl == null || proxyUrl.isEmpty()) && testHttpProxy != null && !testHttpProxy.isEmpty()) {
        proxyUrl = testHttpProxy;
      }

      if (proxyUrl != null && !proxyUrl.isEmpty()) {
        URI proxyUri = UriUtils.buildUri(proxyUrl);
        defaultConfig.host = proxyUri.getHost();
        defaultConfig.port = proxyUri.getPort();
        if (proxyUri.getUserInfo() != null && !proxyUri.getUserInfo().isEmpty()) {
          String[] userInfo = proxyUri.getUserInfo().split(":");
          defaultConfig.user = userInfo[0];
          defaultConfig.password = userInfo[1];
        }
      }

      return defaultConfig;
    }
  }

  public String getHost() {
    return this.host;
  }

  public int getPort() {
    return this.port;
  }

  public String getUser() {
    return this.user;
  }

  public String getPassword() {
    return this.password;
  }

  public String getHostList() {
    return this.hostList;
  }

  public Set<String> getHosts() {
    return this.hosts;
  }

  static Set<String> parseHostList(String hostList) {
    if (hostList != null && !hostList.isEmpty()) {
      List<String> tokens = Arrays.asList(hostList.split(","));
      tokens = (List)tokens.stream().map(String::trim).filter((s) -> {
        return !s.isEmpty();
      }).collect(Collectors.toList());
      return new HashSet(tokens);
    } else {
      return Collections.emptySet();
    }
  }
}
