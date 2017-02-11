package com.wavefront.integrations.metrics;

import com.google.common.base.Throwables;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Creates a socket with a buffered-writer around it. The socket can try to reconnect on
 * unexpected remote terminations.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class ReconnectingSocket {
  protected static final Logger logger = Logger.getLogger(SocketMetricsProcessor.class.getCanonicalName());

  private final String host;
  private final int port;
  private BufferedOutputStream stream;

  /**
   * @throws IOException When we cannot open the remote socket.
   */
  public ReconnectingSocket(String host, int port) throws IOException {
    this.host = host;
    this.port = port;
    resetSocket();
  }

  /**
   * Closes the stream best-effort. Guaranteed to re-instantiate the stream.
   */
  private void resetSocket() throws IOException {
    try {
      if (stream != null) {
        stream.close();
      }
    } finally {
      stream = new BufferedOutputStream(new Socket(this.host, this.port).getOutputStream());
      logger.log(Level.INFO, String.format("Successfully reset connection to %s:%d", this.host, this.port));
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
      stream.write(message.getBytes());
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
}
