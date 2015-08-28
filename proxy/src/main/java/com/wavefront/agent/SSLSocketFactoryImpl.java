package com.wavefront.agent;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Delegated SSLSocketFactory that sets SoTimeout explicitly.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class SSLSocketFactoryImpl extends SSLSocketFactory {
  private final SSLSocketFactory delegate;
  private final int soTimeout;

  public SSLSocketFactoryImpl(SSLSocketFactory delegate, int soTimeoutMs) {
    this.delegate = delegate;
    this.soTimeout = soTimeoutMs;
  }

  @Override
  public Socket createSocket(Socket socket, String s, int i, boolean b) throws IOException {
    Socket socket1 = delegate.createSocket(socket, s, i, b);
    socket1.setSoTimeout(soTimeout);
    return socket1;
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return delegate.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return delegate.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket() throws IOException {
    Socket socket = delegate.createSocket();
    socket.setSoTimeout(soTimeout);
    return socket;
  }

  @Override
  public Socket createSocket(InetAddress inetAddress, int i) throws IOException {
    Socket socket = delegate.createSocket(inetAddress, i);
    socket.setSoTimeout(soTimeout);
    return socket;
  }

  @Override
  public Socket createSocket(InetAddress inetAddress, int i, InetAddress inetAddress1, int i1) throws IOException {
    Socket socket = delegate.createSocket(inetAddress, i, inetAddress1, i1);
    socket.setSoTimeout(soTimeout);
    return socket;
  }

  @Override
  public Socket createSocket(String s, int i) throws IOException, UnknownHostException {
    Socket socket = delegate.createSocket(s, i);
    socket.setSoTimeout(soTimeout);
    return socket;
  }

  @Override
  public Socket createSocket(String s, int i, InetAddress inetAddress, int i1) throws IOException,
      UnknownHostException {
    Socket socket = delegate.createSocket(s, i, inetAddress, i1);
    socket.setSoTimeout(soTimeout);
    return socket;
  }
}
