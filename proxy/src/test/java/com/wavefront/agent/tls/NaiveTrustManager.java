package com.wavefront.agent.tls;

import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;

public class NaiveTrustManager implements X509TrustManager {
  public void checkClientTrusted(X509Certificate[] cert, String authType) {}

  public void checkServerTrusted(X509Certificate[] cert, String authType) {}

  public X509Certificate[] getAcceptedIssuers() {
    return null;
  }
}
