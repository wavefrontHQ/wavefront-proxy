package com.wavefront.integrations;

import com.wavefront.metrics.ReconnectingSocket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;

/**
 * Base class for sending data to a Wavefront proxy port.
 *
 * @author Clement Pang (clement@wavefront.com).
 * @author Vikram Raman (vikram@wavefront.com).
 * @author Han Zhang (zhanghan@vmware.com).
 */
public class ProxyConnectionHandler implements WavefrontConnectionHandler {

  private final InetSocketAddress address;
  private final SocketFactory socketFactory;
  private volatile ReconnectingSocket reconnectingSocket;
  private final AtomicInteger failures;

  protected ProxyConnectionHandler(InetSocketAddress address, SocketFactory socketFactory) {
    this.address = address;
    this.socketFactory = socketFactory;
    this.reconnectingSocket = null;
    failures = new AtomicInteger();
  }

  @Override
  public synchronized void connect() throws IllegalStateException, IOException {
    if (reconnectingSocket != null) {
      throw new IllegalStateException("Already connected");
    }
    try {
      reconnectingSocket = new ReconnectingSocket(address.getHostName(), address.getPort(), socketFactory);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isConnected() {
    return reconnectingSocket != null;
  }

  @Override
  public void flush() throws IOException {
    if (reconnectingSocket != null) {
      reconnectingSocket.flush();
    }
  }

  @Override
  public int getFailureCount() {
    return failures.get();
  }

  public int incrementAndGetFailureCount() {
    return failures.incrementAndGet();
  }

  @Override
  public synchronized void close() throws IOException {
    if (reconnectingSocket != null) {
      reconnectingSocket.close();
      reconnectingSocket = null;
    }
  }

  /**
   * Sends the given data to the Wavefront proxy.
   *
   * @param lineData line data in a Wavefront supported format
   * @throws Exception If there was failure sending the data
   */
  protected void sendData(String lineData) throws Exception {
    reconnectingSocket.write(lineData);
  }
}
