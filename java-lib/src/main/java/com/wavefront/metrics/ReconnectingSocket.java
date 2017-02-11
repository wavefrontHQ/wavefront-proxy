package com.wavefront.metrics;

import com.google.common.base.Throwables;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.SocketFactory;

/**
 * Creates a socket with a buffered-writer around it. The socket can try to reconnect on
 * unexpected remote terminations.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class ReconnectingSocket {
  protected static final Logger logger = Logger.getLogger(ReconnectingSocket.class.getCanonicalName());

  private final String host;
  private final int port;
  private final SocketFactory socketFactory;
  private Socket underlyingSocket;
  private BufferedOutputStream stream;

  /**
   * @throws IOException When we cannot open the remote socket.
   */
  public ReconnectingSocket(String host, int port, SocketFactory socketFactory) throws IOException {
    this.host = host;
    this.port = port;
    this.socketFactory = socketFactory;
    this.stream = null;
    resetSocket();
  }

  public ReconnectingSocket(String host, int port) throws IOException {
    this(host, port, SocketFactory.getDefault());
  }

  /**
   * Closes the stream best-effort. Tries to re-instantiate the stream.
   *
   * @throws IOException          If we cannot close a stream we had opened before.
   * @throws UnknownHostException When {@link #host} and {@link #port} are bad.
   */
  private void resetSocket() throws IOException {
    try {
      if (stream != null) {
        stream.close();
      }
    } finally {
      underlyingSocket = socketFactory.createSocket(host, port);
      stream = new BufferedOutputStream(underlyingSocket.getOutputStream());
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
      stream.write(message.getBytes());  // Might be NPE due to previously failed call to resetSocket.
    } catch (Exception e) {
      try {
        logger.log(Level.WARNING, "Attempting to reset socket connection.", e);
        resetSocket();
        stream.write(message.getBytes());
      } catch (Exception e2) {
        throw Throwables.propagate(e2);
      }
    }
  }

  /**
   * Flushes the stream best-effort. If that fails, we reset the connection.
   */
  public void flush() throws IOException {
    try {
      stream.flush();
    } catch (Exception e) {
      logger.log(Level.WARNING, "Attempting to reset socket connection.", e);
      resetSocket();
    }
  }

  public void close() throws IOException {
    try {
      flush();
    } finally {
      stream.close();
    }
  }
}
