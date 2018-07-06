package com.wavefront.integrations;

import java.io.Closeable;
import java.io.IOException;

/**
 * Wavefront Client that sends data to a Wavefront proxy or Wavefront service.
 *
 * @author Vikram Raman (vikram@wavefront.com)
 */
public interface WavefrontConnectionHandler extends Closeable {

  /**
   * Connects to the server.
   *
   * @throws IllegalStateException if the client is already connected
   * @throws IOException           if there is an error connecting
   */
  void connect() throws IllegalStateException, IOException;

  /**
   * Flushes buffer, if applicable
   *
   * @throws IOException
   */
  void flush() throws IOException;

  /**
   * Returns true if ready to send data
   */
  boolean isConnected();

  /**
   * Returns the number of failed writes to the server.
   *
   * @return the number of failed writes to the server
   */
  int getFailureCount();
}
