package com.wavefront.integrations;

import com.wavefront.common.WavefrontDataFormat;
import com.wavefront.common.HistogramGranularity;
import com.wavefront.common.Pair;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  private final ProxyConnectionHandler metricsProxyConnectionHandler;
  private final ProxyConnectionHandler distributionProxyConnectionHandler;

  /**
   * Source to use if there's none.
   */
  private String source;

  /**
   *
   *
   * @param agentHostName     The hostname of the Wavefront Proxy Agent
   * @param metricsPort       The port of the Wavefront Proxy Agent that receives metrics
   */
  public Wavefront(String agentHostName, int metricsPort) {
    this(agentHostName, metricsPort, null, SocketFactory.getDefault());
  }

  /**
   * Creates a new client which connects to the given address using the default
   * {@link SocketFactory}.
   *
   * @param agentHostName     The hostname of the Wavefront Proxy Agent
   * @param metricsPort       The port of the Wavefront Proxy Agent that receives metrics
   * @param distributionPort  The port of the Wavefront Proxy Agent that receives histogram distributions
   */
  public Wavefront(String agentHostName, int metricsPort, @Nullable Integer distributionPort) {
    this(agentHostName, metricsPort, distributionPort, SocketFactory.getDefault());
  }

  /**
   * Creates a new client which connects to the given address and socket factory.
   *
   * @param agentHostName     The hostname of the Wavefront Proxy Agent
   * @param metricsPort       The port of the Wavefront Proxy Agent that receives metrics
   * @param distributionPort  The port of the Wavefront Proxy Agent that receives histogram distributions
   * @param socketFactory     The socket factory
   */
  public Wavefront(String agentHostName, int metricsPort, @Nullable Integer distributionPort, SocketFactory
      socketFactory) {
    this(new InetSocketAddress(agentHostName, metricsPort),
        distributionPort != null ? new InetSocketAddress(agentHostName, distributionPort) : null,
        socketFactory);
  }

  /**
   * Creates a new client which connects to the given address using the default
   * {@link SocketFactory}.
   *
   * @param metricsAgentAddress       The address of the Wavefront Proxy Agent that receives metrics
   * @param distributionAgentAddress  The address of the Wavefront Proxy Agent that receives histogram distributions
   */
  public Wavefront(InetSocketAddress metricsAgentAddress, @Nullable InetSocketAddress distributionAgentAddress) {
    metricsProxyConnectionHandler = new ProxyConnectionHandler(metricsAgentAddress, SocketFactory.getDefault());
    distributionProxyConnectionHandler = distributionAgentAddress != null ? new ProxyConnectionHandler
        (distributionAgentAddress, SocketFactory.getDefault()) : null;
  }

  /**
   * Creates a new client which connects to the given address and socket factory using the given
   * character set.
   *
   * @param metricsAgentAddress       The address of the Wavefront Proxy Agent that receives metrics
   * @param distributionAgentAddress  The address of the Wavefront Proxy Agent that receives histogram distributions
   * @param socketFactory             The socket factory
   */
  public Wavefront(InetSocketAddress metricsAgentAddress, @Nullable InetSocketAddress distributionAgentAddress,
                   SocketFactory socketFactory) {
    metricsProxyConnectionHandler = new ProxyConnectionHandler(metricsAgentAddress, socketFactory);
    distributionProxyConnectionHandler = distributionAgentAddress != null ? new ProxyConnectionHandler
        (distributionAgentAddress, socketFactory) : null;
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
  public void send(Set<HistogramGranularity> histogramGranularities, List<Pair<Double, Integer>> distribution, String name)
      throws IOException {
    internalSend(histogramGranularities, null, distribution, name, source, null);
  }

  @Override
  public void send(Set<HistogramGranularity> histogramGranularities, @Nullable Long timestamp,
                   List<Pair<Double, Integer>> distribution, String name) throws IOException {
    internalSend(histogramGranularities, timestamp, distribution, name, source, null);
  }

  @Override
  public void send(Set<HistogramGranularity> histogramGranularities, @Nullable Long timestamp,
                   List<Pair<Double, Integer>>
      distribution, String name, String source) throws IOException {
    internalSend(histogramGranularities, timestamp, distribution, name, source, null);
  }

  @Override
  public void send(Set<HistogramGranularity> histogramGranularities,
                   List<Pair<Double, Integer>> distribution, String name, String source,
                   @Nullable Map<String, String> pointTags) throws IOException {
    internalSend(histogramGranularities, null, distribution, name, source, pointTags);
  }

  @Override
  public void send(Set<HistogramGranularity> histogramGranularities, @Nullable Long timestamp,
                   List<Pair<Double, Integer>> distribution, String name, String source,
                   @Nullable Map<String, String> pointTags) throws IOException {
    internalSend(histogramGranularities, timestamp, distribution, name, source, pointTags);
  }

  private void internalSend(String name, double value, @Nullable Long timestamp, String source,
                            @Nullable Map<String, String> pointTags) throws IOException {
    if (!metricsProxyConnectionHandler.isConnected()) {
      try {
        metricsProxyConnectionHandler.connect();
      } catch (IllegalStateException ex) {
        // already connected.
      }
    }
    String point = WavefrontDataFormat.pointToString(name, value, timestamp, source, pointTags, true);
    try {
      try {
        metricsProxyConnectionHandler.sendData(point);
      } catch (Exception e) {
        throw new IOException(e);
      }
    } catch (IOException e) {
      metricsProxyConnectionHandler.incrementAndGetFailureCount();
      throw e;
    }
  }

  private void internalSend(Set<HistogramGranularity> histogramGranularities, @Nullable Long timestamp,
                            List<Pair<Double, Integer>> distribution, String name, String source,
                            @Nullable Map<String, String> pointTags) throws IOException {
    if (distribution == null || distribution.isEmpty()) {
      return; // don't send if distribution is empty
    }
    if (!distributionProxyConnectionHandler.isConnected()) {
      try {
        distributionProxyConnectionHandler.connect();
      } catch (IllegalStateException ex) {
        // already connected.
      }
    }
    List<String> histograms = WavefrontDataFormat.histogramToStrings(histogramGranularities, timestamp, distribution,
        name, source, pointTags, true);
    for (String histogram : histograms) {
      try {
        try {
          distributionProxyConnectionHandler.sendData(histogram);
        } catch (Exception e) {
          throw new IOException(e);
        }
      } catch (IOException e) {
        distributionProxyConnectionHandler.incrementAndGetFailureCount();
        throw e;
      }
    }

  }

  @Override
  public void connect() throws IllegalStateException, IOException {
    metricsProxyConnectionHandler.connect();
    if (canHandleDistributions()) {
      distributionProxyConnectionHandler.connect();
    }
  }

  @Override
  public void flush() throws IOException {
    metricsProxyConnectionHandler.flush();
    if (canHandleDistributions()) {
      distributionProxyConnectionHandler.flush();
    }
  }

  @Override
  public boolean isConnected() {
    return metricsProxyConnectionHandler.isConnected() && (!canHandleDistributions() ||
        distributionProxyConnectionHandler.isConnected());
  }

  @Override
  public int getFailureCount() {
    return metricsProxyConnectionHandler.getFailureCount() + (canHandleDistributions() ?
        distributionProxyConnectionHandler.getFailureCount() : 0);
  }

  @Override
  public void close() throws IOException {
    metricsProxyConnectionHandler.close();
    if (canHandleDistributions()) {
      distributionProxyConnectionHandler.close();
    }
  }

  public boolean canHandleDistributions() {
    return distributionProxyConnectionHandler != null;
  }
}
