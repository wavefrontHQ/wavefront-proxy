package com.wavefront.integrations;

import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import javax.net.SocketFactory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Wavefront Client that sends data directly via TCP to the Wavefront Proxy Agent.
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

  private Socket socket;
  private Writer writer;
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
  }

  private void initializeSource() throws UnknownHostException {
    if (source == null) {
      source = InetAddress.getLocalHost().getHostName();
    }
  }

  @Override
  public synchronized void connect() throws IllegalStateException, IOException {
    if (socket != null) {
      throw new IllegalStateException("Already connected");
    }
    InetSocketAddress address = this.address;
    if (address.getAddress() == null) {
      throw new UnknownHostException(address.getHostName());
    }

    this.socket = socketFactory.createSocket(address.getAddress(), address.getPort());
    this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), UTF_8));
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
    return socket != null && socket.isConnected() && !socket.isClosed();
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
    if (StringUtils.isBlank(name)) {
      throw new IllegalArgumentException("metric name cannot be blank");
    }
    if (StringUtils.isBlank(source)) {
      throw new IllegalArgumentException("source cannot be blank");
    }
    try {
      writer.write(sanitize(name));
      writer.write(' ');
      writer.write(Double.toString(value));
      if (timestamp != null) {
        writer.write(' ');
        writer.write(Long.toString(timestamp));
      }
      writer.write(" host=");
      writer.write(sanitize(source));
      if (pointTags != null) {
        for (Map.Entry<String, String> tag : pointTags.entrySet()) {
          if (StringUtils.isBlank(tag.getKey())) {
            throw new IllegalArgumentException("point tag key cannot be blank");
          }
          if (StringUtils.isBlank(tag.getValue())) {
            throw new IllegalArgumentException("point tag value cannot be blank");
          }
          writer.write(" ");
          writer.write(sanitize(tag.getKey()));
          writer.write("=");
          writer.write(sanitize(tag.getValue()));
        }
      }
      writer.write('\n');
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
    if (writer != null) {
      writer.flush();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    flush();
    if (socket != null) {
      socket.close();
    }
    this.socket = null;
    this.writer = null;
  }

  static String sanitize(String s) {
    final String whitespaceSanitized = WHITESPACE.matcher(s).replaceAll("-");
    if (s.contains("\"") || s.contains("'")) {
      // for single quotes, once we are double-quoted, single quotes can exist happily inside it.
      return "\"" + whitespaceSanitized.replaceAll("\"", "\\\\\"") + "\"";
    } else {
      return whitespaceSanitized;
    }
  }
}
