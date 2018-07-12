package com.wavefront.integrations;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Wavefront Client that sends data directly via TCP to the Wavefront Proxy Agent.
 *
 * @author Clement Pang (clement@wavefront.com).
 * @author Conor Beverland (conor@wavefront.com).
 */
public interface WavefrontSender extends WavefrontConnectionHandler, Closeable {

  /**
   * Send a measurement to Wavefront. The current machine's hostname would be used as the source. The point will be
   * timestamped at the agent.
   *
   * @param name  The name of the metric. Spaces are replaced with '-' (dashes) and quotes will be automatically
   *              escaped.
   * @param value The value to be sent.
   * @throws IOException          Throws if there was an error sending the metric.
   * @throws UnknownHostException Throws if there's an error determining the current host.
   */
  void send(String name, double value) throws IOException;

  /**
   * Send a measurement to Wavefront. The current machine's hostname would be used as the source.
   *
   * @param name      The name of the metric. Spaces are replaced with '-' (dashes) and quotes will be automatically
   *                  escaped.
   * @param value     The value to be sent.
   * @param timestamp The timestamp in seconds since the epoch to be sent.
   * @throws IOException          Throws if there was an error sending the metric.
   * @throws UnknownHostException Throws if there's an error determining the current host.
   */
  void send(String name, double value, @Nullable Long timestamp) throws IOException;

  /**
   * Send a measurement to Wavefront.
   *
   * @param name      The name of the metric. Spaces are replaced with '-' (dashes) and quotes will be automatically
   *                  escaped.
   * @param value     The value to be sent.
   * @param timestamp The timestamp in seconds since the epoch to be sent.
   * @param source    The source (or host) that's sending the metric.
   * @throws IOException if there was an error sending the metric.
   */
  void send(String name, double value, @Nullable Long timestamp, String source) throws IOException;

  /**
   * Send the given measurement to the server.
   *
   * @param name      The name of the metric. Spaces are replaced with '-' (dashes) and quotes will be automatically
   *                  escaped.
   * @param value     The value to be sent.
   * @param source    The source (or host) that's sending the metric. Null to use machine hostname.
   * @param pointTags The point tags associated with this measurement.
   * @throws IOException if there was an error sending the metric.
   */
  void send(String name, double value, String source, @Nullable Map<String, String> pointTags) throws IOException;

  /**
   * Send the given measurement to the server.
   *
   * @param name      The name of the metric. Spaces are replaced with '-' (dashes) and quotes will be automatically
   *                  escaped.
   * @param value     The value to be sent.
   * @param timestamp The timestamp in seconds since the epoch to be sent. Null to use agent assigned timestamp.
   * @param source    The source (or host) that's sending the metric. Null to use machine hostname.
   * @param pointTags The point tags associated with this measurement.
   * @throws IOException if there was an error sending the metric.
   */
  void send(String name, double value, @Nullable Long timestamp, String source,
            @Nullable Map<String, String> pointTags) throws IOException;
}
