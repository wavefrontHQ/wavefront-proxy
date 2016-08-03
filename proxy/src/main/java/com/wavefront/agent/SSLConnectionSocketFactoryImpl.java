package com.wavefront.agent;

import org.apache.http.HttpHost;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Delegated SSLConnectionSocketFactory that sets SoTimeout explicitly (for Apache HttpClient).
 *
 * @author vasily@wavefront.com
 */
public class SSLConnectionSocketFactoryImpl implements LayeredConnectionSocketFactory {
  private final SSLConnectionSocketFactory delegate;
  private final int soTimeout;

  public SSLConnectionSocketFactoryImpl(SSLConnectionSocketFactory delegate, int soTimeoutMs) {
    this.delegate = delegate;
    this.soTimeout = soTimeoutMs;
  }

  @Override
  public Socket createSocket(HttpContext context) throws IOException {
    Socket socket1 = delegate.createSocket(context);
    socket1.setSoTimeout(soTimeout);
    return socket1;
  }

  @Override
  public Socket connectSocket(int connectTimeout, Socket sock, HttpHost host, InetSocketAddress remoteAddress,
                              InetSocketAddress localAddress, HttpContext context) throws IOException {
    Socket socket1 = delegate.connectSocket(soTimeout, sock, host, remoteAddress, localAddress, context);
    socket1.setSoTimeout(soTimeout);
    return socket1;
  }

  @Override
  public Socket createLayeredSocket(Socket socket, String target, int port, HttpContext context) throws IOException {
    Socket socket1 = delegate.createLayeredSocket(socket, target, port, context);
    socket1.setSoTimeout(soTimeout);
    return socket1;
  }
}
