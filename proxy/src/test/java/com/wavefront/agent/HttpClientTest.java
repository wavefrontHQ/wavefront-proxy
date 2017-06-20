package com.wavefront.agent;

import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.client.jaxrs.engines.ApacheHttpClient4Engine;
import org.jboss.resteasy.plugins.providers.jackson.ResteasyJackson2Provider;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import static org.junit.Assert.assertTrue;


public final class HttpClientTest {

  @Path("")
  public interface SimpleRESTEasyAPI {
    @GET
    @Path("search")
    @Produces(MediaType.TEXT_HTML)
    void search(@QueryParam("q") String query);
  }

  class SocketServerRunnable implements Runnable {
    private ServerSocket server;

    public int getPort() {
      return server.getLocalPort();
    }

    public SocketServerRunnable() throws IOException {
      server = new ServerSocket(0);
    }

    public void run() {
      try {
        Socket sock = server.accept();
        sock.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Test(expected=ProcessingException.class)
  public void httpClientTimeoutsWork() throws Exception {
    ResteasyProviderFactory factory = ResteasyProviderFactory.getInstance();
    factory.registerProvider(JsonNodeWriter.class);
    factory.registerProvider(ResteasyJackson2Provider.class);

    HttpClient httpClient = HttpClientBuilder.create().
        useSystemProperties().
        setMaxConnTotal(200).
        setMaxConnPerRoute(100).
        setConnectionTimeToLive(1, TimeUnit.MINUTES).
        setDefaultSocketConfig(
            SocketConfig.custom().
                setSoTimeout(100).build()).
        setDefaultRequestConfig(
            RequestConfig.custom().
                setContentCompressionEnabled(true).
                setRedirectsEnabled(true).
                setConnectTimeout(5000).
                setConnectionRequestTimeout(5000).
                setSocketTimeout(60000).build()).
        setSSLSocketFactory(
            new LayeredConnectionSocketFactory() {
              @Override
              public Socket createLayeredSocket(Socket socket, String target, int port, HttpContext context)
                  throws IOException, UnknownHostException {
                return SSLConnectionSocketFactory.getSystemSocketFactory()
                    .createLayeredSocket(socket, target, port, context);
              }

              @Override
              public Socket createSocket(HttpContext context) throws IOException {
                return SSLConnectionSocketFactory.getSystemSocketFactory()
                    .createSocket(context);
              }

              @Override
              public Socket connectSocket(int connectTimeout, Socket sock, HttpHost host, InetSocketAddress remoteAddress, InetSocketAddress localAddress, HttpContext context) throws IOException {
                assertTrue("Non-zero timeout passed to connect socket is expected", connectTimeout > 0);
                throw new ProcessingException("OK");
              }
            }).build();

    ResteasyClient client = new ResteasyClientBuilder().
        httpEngine(new ApacheHttpClient4Engine(httpClient, true)).
        providerFactory(factory).
        build();

    SocketServerRunnable sr = new SocketServerRunnable();
    Thread serverThread = new Thread(sr);
    serverThread.start();

    ResteasyWebTarget target = client.target("https://localhost:" + sr.getPort());
    SimpleRESTEasyAPI proxy = target.proxy(SimpleRESTEasyAPI.class);
    proxy.search("resteasy");

  }

}
