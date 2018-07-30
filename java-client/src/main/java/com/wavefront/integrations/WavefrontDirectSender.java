package com.wavefront.integrations;

import com.wavefront.common.HistogramGranularity;
import com.wavefront.common.Pair;
import com.wavefront.common.WavefrontDataFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(WavefrontDirectSender.class);
  private static final int MAX_QUEUE_SIZE = 50000;
  private static final int BATCH_SIZE = 10000;

  // buffer for sending points
  private final LinkedBlockingQueue<String> pointBuffer = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

  // a separate buffer for sending histogram distributions because they require a different format parameter
  private final LinkedBlockingQueue<String> histogramBuffer = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

  private final AtomicInteger failures = new AtomicInteger();
  private String defaultSource;

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
    addPoint(name, value, null, getDefaultSource(), null);
  }

  @Override
  public void send(String name, double value, @Nullable Long timestamp) throws IOException {
    addPoint(name, value, timestamp, getDefaultSource(), null);
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
  public void send(Set<HistogramGranularity> histogramGranularities, List<Pair<Double, Integer>> centroids, String name)
      throws IOException {
    addHistogram(histogramGranularities, null, centroids, name, getDefaultSource(), null);
  }

  @Override
  public void send(Set<HistogramGranularity> histogramGranularities, @Nullable Long timestamp,
                   List<Pair<Double, Integer>> centroids, String name) throws IOException {
    addHistogram(histogramGranularities, timestamp, centroids, name, getDefaultSource(), null);
  }

  @Override
  public void send(Set<HistogramGranularity> histogramGranularities, @Nullable Long timestamp,
                   List<Pair<Double, Integer>> centroids, String name, String source) throws IOException {
    addHistogram(histogramGranularities, timestamp, centroids, name, source, null);
  }

  @Override
  public void send(Set<HistogramGranularity> histogramGranularities,
                   List<Pair<Double, Integer>> centroids, String name, String source,
                   @Nullable Map<String, String> pointTags) throws IOException {
    addHistogram(histogramGranularities, null, centroids, name, source, pointTags);
  }

  @Override
  public void send(Set<HistogramGranularity> histogramGranularities, @Nullable Long timestamp,
                   List<Pair<Double, Integer>> centroids, String name, String source,
                   @Nullable Map<String, String> pointTags) throws IOException {
    addHistogram(histogramGranularities, timestamp, centroids, name, source, pointTags);
  }

  private void addPoint(@NotNull String name, double value, @Nullable Long timestamp, @NotNull String source,
                        @Nullable Map<String, String> pointTags) throws IOException {
    String point;
    try {
      point = WavefrontDataFormat.pointToString(name, value, timestamp, source, pointTags, false);
    } catch (IllegalArgumentException e) {
      LOGGER.debug("Invalid point: " + e.getMessage());
      point = null;
    }
    if (point != null && !pointBuffer.offer(point)) {
      LOGGER.debug("Buffer full, dropping point " + name);
    }
  }

  private void addHistogram(Set<HistogramGranularity> histogramGranularities, @Nullable Long timestamp,
                            List<Pair<Double, Integer>> centroids, String name, String source,
                            @Nullable Map<String, String> pointTags) throws IOException {
    List<String> histograms;
    try {
      histograms = WavefrontDataFormat.histogramToStrings(histogramGranularities, timestamp, centroids, name, source,
          pointTags, false);
    } catch (IllegalArgumentException e) {
      LOGGER.debug("Invalid histogram: " + e.getMessage());
      histograms = null;
    }
    if (histograms != null) {
      for (String histogram : histograms) {
        while (!histogramBuffer.offer(histogram)) {
          LOGGER.debug("Buffer full, dropping histogram " + name);
        }
      }
    }
  }

  private String getDefaultSource() throws UnknownHostException {
    if (defaultSource == null) {
      defaultSource = InetAddress.getLocalHost().getHostName();
    }
    return defaultSource;
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
