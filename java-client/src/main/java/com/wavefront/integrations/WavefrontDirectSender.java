package com.wavefront.integrations;

import com.wavefront.common.HistogramGranularity;
import com.wavefront.common.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;

import static com.wavefront.api.agent.Constants.PUSH_FORMAT_GRAPHITE_V2;
import static com.wavefront.api.agent.Constants.PUSH_FORMAT_HISTOGRAM;

/**
 * Wavefront Client that sends data directly to Wavefront via the direct ingestion APIs.
 *
 * @author Vikram Raman (vikram@wavefront.com)
 */
public class WavefrontDirectSender extends AbstractDirectConnectionHandler implements WavefrontSender {

  private static final String DEFAULT_SOURCE = "wavefrontDirectSender";
  private static final Logger LOGGER = LoggerFactory.getLogger(WavefrontDirectSender.class);
  private static final String quote = "\"";
  private static final String escapedQuote = "\\\"";
  private static final int MAX_QUEUE_SIZE = 50000;
  private static final int BATCH_SIZE = 10000;

  // buffer for sending points
  private final LinkedBlockingQueue<String> pointBuffer = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

  // a separate buffer for sending histogram distributions because they require a different format parameter
  private final LinkedBlockingQueue<String> histogramBuffer = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

  private final AtomicInteger failures = new AtomicInteger();

  /**
   * Creates a new client that connects directly to a given Wavefront service.
   *
   * @param server A Wavefront server URL of the form "https://clusterName.wavefront.com"
   * @param token A valid API token with direct ingestion permissions
   */
  public WavefrontDirectSender(String server, String token) {
    super(server, token);
  }

  @Override
  public void send(String name, double value) throws IOException {
    addPoint(name, value, null, DEFAULT_SOURCE, null);
  }

  @Override
  public void send(String name, double value, @Nullable Long timestamp) throws IOException {
    addPoint(name, value, timestamp, DEFAULT_SOURCE, null);
  }

  @Override
  public void send(String name, double value, @Nullable Long timestamp, String source) throws IOException {
    addPoint(name, value, timestamp, source, null);
  }

  @Override
  public void send(String name, double value, String source, @Nullable Map<String, String> pointTags) throws IOException {
    addPoint(name, value, null, source, pointTags);
  }

  @Override
  public void send(String name, double value, @Nullable Long timestamp, String source,
                   @Nullable Map<String, String> pointTags) throws IOException {
    addPoint(name, value, timestamp, source, pointTags);
  }

  @Override
  public void send(HistogramGranularity histogramGranularity, List<Pair<Double, Integer>> distribution, String name) throws IOException {
    addHistogram(histogramGranularity, null, distribution, name, DEFAULT_SOURCE, null);
  }

  @Override
  public void send(HistogramGranularity histogramGranularity, @Nullable Long timestamp,
                   List<Pair<Double, Integer>> distribution, String name) throws IOException {
    addHistogram(histogramGranularity, timestamp, distribution, name, DEFAULT_SOURCE, null);
  }

  @Override
  public void send(HistogramGranularity histogramGranularity, @Nullable Long timestamp,
                   List<Pair<Double, Integer>> distribution, String name, String source) throws IOException {
    addHistogram(histogramGranularity, timestamp, distribution, name, source, null);
  }

  @Override
  public void send(HistogramGranularity histogramGranularity,
                   List<Pair<Double, Integer>> distribution, String name, String source,
                   @Nullable Map<String, String> pointTags) throws IOException {
    addHistogram(histogramGranularity, null, distribution, name, source, pointTags);
  }

  @Override
  public void send(HistogramGranularity histogramGranularity, @Nullable Long timestamp,
                   List<Pair<Double, Integer>> distribution, String name, String source,
                   @Nullable Map<String, String> pointTags) throws IOException {
    addHistogram(histogramGranularity, timestamp, distribution, name, source, pointTags);
  }

  private void addPoint(@NotNull String name, double value, @Nullable Long timestamp, @NotNull String source,
                        @Nullable Map<String, String> pointTags) throws IOException {
    String point = pointToString(name, value, timestamp, source, pointTags);
    if (point != null && !pointBuffer.offer(point)) {
      LOGGER.debug("Buffer full, dropping point " + name);
    }
  }

  private void addHistogram(HistogramGranularity histogramGranularity, @Nullable Long timestamp,
                            List<Pair<Double, Integer>> distribution, String name, String source,
                            @Nullable Map<String, String> pointTags) throws IOException {
    String histogram = histogramToString(histogramGranularity, timestamp, distribution, name, source, pointTags);
    if (histogram != null && !histogramBuffer.offer(histogram)) {
      LOGGER.debug("Buffer full, dropping histogram " + name);
    }
  }

  private static String escapeQuotes(String raw) {
    return StringUtils.replace(raw, quote, escapedQuote);
  }

  @Nullable
  static String pointToString(String name, double value, @Nullable Long timestamp, String source,
                               @Nullable Map<String, String> pointTags) {

    if (StringUtils.isBlank(name) || StringUtils.isBlank(source)) {
      LOGGER.debug("Invalid point: Empty name/source");
      return null;
    }

    StringBuilder sb = new StringBuilder(quote).
        append(escapeQuotes(name)).append(quote).append(" ").
        append(Double.toString(value)).append(" ");
    if (timestamp != null) {
      sb.append(Long.toString(timestamp)).append(" ");
    }
    sb.append("source=").append(quote).append(escapeQuotes(source)).append(quote);

    sb.append(pointTagsToString(pointTags));
    return sb.toString();
  }

  @Nullable
  static String histogramToString(HistogramGranularity histogramGranularity, @Nullable Long timestamp,
                                  List<Pair<Double, Integer>> distribution, String name, String source,
                                  @Nullable Map<String, String> pointTags) {

    if (distribution == null || distribution.isEmpty()) {
      return null;  // don't send if distribution is empty
    }
    if (histogramGranularity == null) {
      LOGGER.debug("Invalid histogram: Histogram granularity is null");
      return null;
    }
    if (StringUtils.isBlank(name) || StringUtils.isBlank(source)) {
      LOGGER.debug("Invalid histogram: Empty name/source");
      return null;
    }

    StringBuilder sb = new StringBuilder(histogramGranularity.identifier);
    if (timestamp != null) {
      sb.append(" ").append(Long.toString(timestamp));
    }
    for (Pair<Double, Integer> pair : distribution) {
      if (pair == null) {
        LOGGER.debug("Invalid histogram: Distribution pair is null");
        return null;
      }
      if (pair._1 == null) {
        LOGGER.debug("Invalid histogram: Distribution value is null");
        return null;
      }
      if (pair._2 == null) {
        LOGGER.debug("Invalid histogram: Distribution count is null");
        return null;
      }
      if (pair._2 <= 0) {
        LOGGER.debug("Invalid histogram: Distribution count is less than 1");
        return null;
      }
      sb.append(" #").append(Integer.toString(pair._2))
          .append(" ").append(Double.toString(pair._1));
    }
    sb.append(" ").append(quote).append(escapeQuotes(name)).append(quote);
    sb.append(" source=").append(quote).append(escapeQuotes(source)).append(quote);

    sb.append(pointTagsToString(pointTags));
    return sb.toString();
  }

  private static String pointTagsToString(Map<String, String> pointTags) {
    StringBuilder sb = new StringBuilder();
    if (pointTags != null) {
      for (Map.Entry<String, String> entry : pointTags.entrySet()) {
        sb.append(' ').append(quote).append(escapeQuotes(entry.getKey())).append(quote).
            append("=").
            append(quote).append(escapeQuotes(entry.getValue())).append(quote);
      }
    }
    return sb.toString();
  }

  @Override
  protected void internalFlush() throws IOException {
    if (!isConnected()) {
        return;
    }
    internalFlush(pointBuffer, PUSH_FORMAT_GRAPHITE_V2);
    internalFlush(histogramBuffer, PUSH_FORMAT_HISTOGRAM);
  }

  private void internalFlush(LinkedBlockingQueue<String> buffer, String format) throws IOException {
    List<String> points = getBatch(buffer);
    if (points.isEmpty()) {
      return;
    }

    Response response = null;
    try (InputStream is = pointsToStream(points)) {
      response = report(format, is);
      if (response.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR ||
          response.getStatusInfo().getFamily() == Response.Status.Family.CLIENT_ERROR) {
        LOGGER.debug("Error reporting points, respStatus=" + response.getStatus());
        try {
          buffer.addAll(points);
        } catch (Exception ex) {
          // unlike offer(), addAll adds partially and throws an exception if Buffer full
          LOGGER.debug("Buffer full, dropping attempted points");
        }
      }
    } catch (IOException ex) {
      failures.incrementAndGet();
      throw ex;
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  private InputStream pointsToStream(List<String> points) {
    StringBuilder sb = new StringBuilder();
    boolean newLine = false;
    for (String point : points) {
      if (newLine) {
        sb.append("\n");
      }
      sb.append(point);
      newLine = true;
    }
    return new ByteArrayInputStream(sb.toString().getBytes());
  }

  private List<String> getBatch(LinkedBlockingQueue<String> buffer) {
    int blockSize = Math.min(buffer.size(), BATCH_SIZE);
    List<String> points = new ArrayList<>(blockSize);
    buffer.drainTo(points, blockSize);
    return points;
  }

  @Override
  public int getFailureCount() {
    return failures.get();
  }

  @Override
  public void run() {
    try {
      this.internalFlush();
    } catch (Throwable ex) {
      LOGGER.debug("Unable to report to Wavefront", ex);
    }
  }
}
