package com.wavefront.integrations;

import com.wavefront.common.HistogramGranularity;
import com.wavefront.common.Pair;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.List;
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
public class Wavefront extends AbstractProxyConnectionHandler implements WavefrontSender {

  private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");
  // this may be optimistic about Carbon/Wavefront
  private static final Charset UTF_8 = Charset.forName("UTF-8");

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
    super(agentAddress, socketFactory);
  }

  private void initializeSource() throws UnknownHostException {
    if (source == null) {
      source = InetAddress.getLocalHost().getHostName();
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
    internalSend(name, value, timestamp, source, null);
  }

  @Override
  public void send(String name, double value, @Nullable Long timestamp, String source) throws IOException {
    internalSend(name, value, timestamp, source, null);
  }

  @Override
  public void send(String name, double value, String source, @Nullable Map<String, String> pointTags)
      throws IOException {
    internalSend(name, value, null, source, pointTags);
  }

  @Override
  public void send(String name, double value, @Nullable Long timestamp, String source,
                   @Nullable Map<String, String> pointTags) throws IOException {
    internalSend(name, value, timestamp, source, pointTags);
  }

  @Override
  public void send(HistogramGranularity histogramGranularity, List<Pair<Double, Integer>> distribution, String name) throws IOException {
    internalSend(histogramGranularity, null, distribution, name, source, null);
  }

  @Override
  public void send(HistogramGranularity histogramGranularity, @Nullable Long timestamp, List<Pair<Double, Integer>> distribution, String name) throws IOException {
    internalSend(histogramGranularity, timestamp, distribution, name, source, null);
  }

  @Override
  public void send(HistogramGranularity histogramGranularity, @Nullable Long timestamp, List<Pair<Double, Integer>> distribution, String name, String source) throws IOException {
    internalSend(histogramGranularity, timestamp, distribution, name, source, null);
  }

  @Override
  public void send(HistogramGranularity histogramGranularity, List<Pair<Double, Integer>> distribution, String name, String source, @Nullable Map<String, String> pointTags) throws IOException {
    internalSend(histogramGranularity, null, distribution, name, source, pointTags);
  }

  @Override
  public void send(HistogramGranularity histogramGranularity, @Nullable Long timestamp,
                   List<Pair<Double, Integer>> distribution, String name, String source,
                   @Nullable Map<String, String> pointTags) throws IOException {
    internalSend(histogramGranularity, timestamp, distribution, name, source, pointTags);
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
      sb.append(pointTagsToString(pointTags));
      sb.append('\n');
      try {
        sendData(sb.toString());
      } catch (Exception e) {
        throw new IOException(e);
      }
    } catch (IOException e) {
      failures.incrementAndGet();
      throw e;
    }
  }

  private void internalSend(HistogramGranularity histogramGranularity, @Nullable Long timestamp,
                            List<Pair<Double, Integer>> distribution, String name, String source,
                            @Nullable Map<String, String> pointTags) throws IOException {
    if (distribution == null || distribution.isEmpty()) {
      return; // don't send if distribution is empty
    }
    if (!isConnected()) {
      try {
        connect();
      } catch (IllegalStateException ex) {
        // already connected.
      }
    }
    if (histogramGranularity == null) {
      throw new IllegalArgumentException("histogram granularity cannot be null");
    }
    if (isBlank(name)) {
      throw new IllegalArgumentException("metric name cannot be blank");
    }
    if (isBlank(source)) {
      throw new IllegalArgumentException("source cannot be blank");
    }
    final StringBuilder sb = new StringBuilder();
    try {
      sb.append(histogramGranularity.identifier);
      if (timestamp != null) {
        sb.append(" ").append(Long.toString(timestamp));
      }
      for (Pair<Double, Integer> pair : distribution) {
        if (pair == null) {
          throw new IllegalArgumentException("distribution pair cannot be null");
        }
        if (pair._1 == null) {
          throw new IllegalArgumentException("distribution value cannot be null");
        }
        if (pair._2 == null) {
          throw new IllegalArgumentException("distribution count cannot be null");
        }
        if (pair._2 <= 0) {
          throw new IllegalArgumentException("distribution count cannot be less than 1");
        }
        sb.append(" #").append(Integer.toString(pair._2)).append(" ").append(Double.toString(pair._1));
      }
      sb.append(" ").append(sanitize(name));
      sb.append(" host=").append(sanitize(source));
      sb.append(pointTagsToString(pointTags));
      sb.append('\n');
      try {
        sendData(sb.toString());
      } catch (Exception e) {
        throw new IOException(e);
      }
    } catch (IOException e) {
      failures.incrementAndGet();
      throw e;
    }
  }

  private String pointTagsToString(Map<String, String> pointTags) {
    StringBuilder sb = new StringBuilder();
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
    return sb.toString();
  }

  @Override
  public int getFailureCount() {
    return failures.get();
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
