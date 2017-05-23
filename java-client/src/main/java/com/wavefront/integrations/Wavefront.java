package com.wavefront.integrations;

import com.wavefront.metrics.ReconnectingSocket;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.net.SocketFactory;

/**
 * Wavefront Client that sends data directly via TCP to the Wavefront Proxy Agent. User should probably
 * attempt to reconnect when exceptions are thrown from any methods.
 *
 * @author Clement Pang (clement@wavefront.com).
 * @author Conor Beverland (conor@wavefront.com).
 */
public class Wavefront implements WavefrontSender {

  private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");
  // this may be optimistic about Carbon/Wavefront
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private final InetSocketAddress address;
  private final SocketFactory socketFactory;

  private volatile ReconnectingSocket reconnectingSocket;
  private AtomicInteger failures = new AtomicInteger();
  /**
   * Source to use if there's none.
   */
  private String source;

  /**
   * Creates a new client which connects to the given address using the default
   * {@link SocketFactory}.
   *
   * @param agentHostName The hostname of the Wavefront Proxy Agent
   * @param port          The port of the Wavefront Proxy Agent
   */
  public Wavefront(String agentHostName, int port) {
    this(agentHostName, port, SocketFactory.getDefault());
  }

  /**
   * Creates a new client which connects to the given address and socket factory.
   *
   * @param agentHostName The hostname of the Wavefront Proxy Agent
   * @param port          The port of the Wavefront Proxy Agent
   * @param socketFactory the socket factory
   */
  public Wavefront(String agentHostName, int port, SocketFactory socketFactory) {
    this(new InetSocketAddress(agentHostName, port), socketFactory);
  }

  /**
   * Creates a new client which connects to the given address using the default
   * {@link SocketFactory}.
   *
   * @param agentAddress the address of the Wavefront Proxy Agent
   */
  public Wavefront(InetSocketAddress agentAddress) {
    this(agentAddress, SocketFactory.getDefault());
  }

  /**
   * Creates a new client which connects to the given address and socket factory using the given
   * character set.
   *
   * @param agentAddress  the address of the Wavefront Proxy Agent
   * @param socketFactory the socket factory
   */
  public Wavefront(InetSocketAddress agentAddress, SocketFactory socketFactory) {
    this.address = agentAddress;
    this.socketFactory = socketFactory;
    this.reconnectingSocket = null;
  }

  private void initializeSource() throws UnknownHostException {
    if (source == null) {
      source = InetAddress.getLocalHost().getHostName();
    }
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
  public void send(String name, double value) throws IOException {
    initializeSource();
    internalSend(name, value, null, source, null);
  }

  @Override
  public void send(String name, double value, @Nullable Long timestamp) throws IOException {
    initializeSource();
    internalSend(name, value, null, source, null);
  }

  @Override
  public void send(String name, double value, @Nullable Long timestamp, String source) throws IOException {
    internalSend(name, value, timestamp, source, null);
  }

  @Override
  public void send(String name, double value, @Nullable Long timestamp, String source,
                   @Nullable Map<String, String> pointTags) throws IOException {
    internalSend(name, value, timestamp, source, pointTags);
  }

  public boolean isConnected() {
    return reconnectingSocket != null;
  }

  private void internalSend(String name, double value, @Nullable Long timestamp, String source,
                            @Nullable Map<String, String> pointTags) throws IOException {
    if (!isConnected()) {
      try {
        connect();
      } catch (IllegalStateException ex) {
        // already connected.
      }
    }
    if (isBlank(name)) {
      throw new IllegalArgumentException("metric name cannot be blank");
    }
    if (isBlank(source)) {
      throw new IllegalArgumentException("source cannot be blank");
    }
    final StringBuilder sb = new StringBuilder();
    try {
      sb.append(sanitize(name));
      sb.append(' ');
      sb.append(Double.toString(value));
      if (timestamp != null) {
        sb.append(' ');
        sb.append(Long.toString(timestamp));
      }
      sb.append(" host=");
      sb.append(sanitize(source));
      if (pointTags != null) {
        for (final Map.Entry<String, String> tag : pointTags.entrySet()) {
          if (isBlank(tag.getKey())) {
            throw new IllegalArgumentException("point tag key cannot be blank");
          }
          if (isBlank(tag.getValue())) {
            throw new IllegalArgumentException("point tag value cannot be blank");
          }
          sb.append(' ');
          sb.append(sanitize(tag.getKey()));
          sb.append('=');
          sb.append(sanitize(tag.getValue()));
        }
      }
      sb.append('\n');
      try {
        reconnectingSocket.write(sb.toString());
      } catch (Exception e) {
        throw new IOException(e);
      }
    } catch (IOException e) {
      failures.incrementAndGet();
      throw e;
    }
  }

  @Override
  public int getFailureCount() {
    return failures.get();
  }

  @Override
  public void flush() throws IOException {
    if (reconnectingSocket != null) {
      reconnectingSocket.flush();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (reconnectingSocket != null) {
      reconnectingSocket.close();
      reconnectingSocket = null;
    }
  }

  static String sanitize(String s) {
    final String whitespaceSanitized = WHITESPACE.matcher(s).replaceAll("-");
    if (s.contains("\"") || s.contains("'")) {
      // for single quotes, once we are double-quoted, single quotes can exist happily inside it.
      return "\"" + whitespaceSanitized.replaceAll("\"", "\\\\\"") + "\"";
    } else {
      return "\"" + whitespaceSanitized + "\"";
    }
  }

  private static boolean isBlank(String s) {
    if (s == null || s.isEmpty()) {
      return true;
    }
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isWhitespace(s.charAt(i))) {
        return false;
      }
    }
    return true;
  }
}
