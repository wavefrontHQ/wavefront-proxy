package com.wavefront.metrics;

import com.google.common.base.Throwables;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.SocketFactory;

/**
 * Creates a TCP client suitable for the WF proxy. That is: a client which is long-lived and semantically one-way.
 * This client tries persistently to reconnect to the given host and port if a connection is ever broken. If the server
 * (in practice, the WF proxy) sends a TCP FIN or TCP RST, we will treat it as a "broken connection" and just try
 * to connect again on the next call to write(). This means each ReconnectingSocket has a polling thread for the server
 * to listen for connection resets.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class ReconnectingSocket {
  protected static final Logger logger = Logger.getLogger(ReconnectingSocket.class.getCanonicalName());

  private final String host;
  private final int port;
  private final SocketFactory socketFactory;
  private final AtomicBoolean serverTerminated;
  private final Thread pollingThread;
  private AtomicReference<Socket> underlyingSocket;
  private AtomicReference<BufferedOutputStream> socketOutputStream;

  /**
   * @throws IOException When we cannot open the remote socket.
   */
  public ReconnectingSocket(String host, int port, SocketFactory socketFactory) throws IOException {
    this.host = host;
    this.port = port;
    this.serverTerminated = new AtomicBoolean(false);
    this.socketFactory = socketFactory;

    this.underlyingSocket = new AtomicReference<>(socketFactory.createSocket(host, port));
    this.underlyingSocket.get().setSoTimeout(2000);
    this.socketOutputStream = new AtomicReference<>(new BufferedOutputStream(underlyingSocket.get().getOutputStream()));

    this.pollingThread = new Thread(() -> {
      byte[] message = new byte[1000];
      int bytesRead;
      while (!Thread.currentThread().isInterrupted()) {
        try {
          bytesRead = underlyingSocket.get().getInputStream().read(message);
        } catch (IOException e) {
          // Read timeout, just try again later. Important to set SO_TIMEOUT elsewhere.
          continue;
        }
        if (bytesRead == -1) {
          serverTerminated.set(true);
          break;
        }
      }
    });

    this.pollingThread.start();
  }

  public ReconnectingSocket(String host, int port) throws IOException {
    this(host, port, SocketFactory.getDefault());
  }

  /**
   * Closes the outputStream best-effort. Tries to re-instantiate the outputStream.
   *
   * @throws IOException          If we cannot close a outputStream we had opened before.
   * @throws UnknownHostException When {@link #host} and {@link #port} are bad.
   */
  private synchronized void resetSocket() throws IOException {
    try {
      BufferedOutputStream old = socketOutputStream.get();
      if (old != null) old.close();
    } catch (SocketException e) {
      logger.log(Level.INFO, "Could not flush to socket.", e);
    } finally {
      serverTerminated.set(false);
      underlyingSocket.getAndSet(socketFactory.createSocket(host, port)).close();
      underlyingSocket.get().setSoTimeout(2000);
      socketOutputStream.set(new BufferedOutputStream(underlyingSocket.get().getOutputStream()));
      logger.log(Level.INFO, String.format("Successfully reset connection to %s:%d", host, port));
    }
  }

  /**
   * Try to send the given message. On failure, reset and try again. If _that_ fails,
   * just rethrow the exception.
   *
   * @throws Exception when a single retry is not enough to have a successful write to the remote host.
   */
  public void write(String message) throws Exception {
    try {
      if (serverTerminated.get()) {
        throw new Exception("Remote server terminated.");  // Handled below.
      }
      // Might be NPE due to previously failed call to resetSocket.
      socketOutputStream.get().write(message.getBytes());
    } catch (Exception e) {
      try {
        logger.log(Level.WARNING, "Attempting to reset socket connection.", e);
        resetSocket();
        socketOutputStream.get().write(message.getBytes());
      } catch (Exception e2) {
        throw Throwables.propagate(e2);
      }
    }
  }

  /**
   * Flushes the outputStream best-effort. If that fails, we reset the connection.
   */
  public void flush() throws IOException {
    try {
      socketOutputStream.get().flush();
    } catch (Exception e) {
      logger.log(Level.WARNING, "Attempting to reset socket connection.", e);
      resetSocket();
    }
  }

  public void close() throws IOException {
    try {
      flush();
    } finally {
      pollingThread.interrupt();
      socketOutputStream.get().close();
    }
  }
}
