package com.wavefront.integrations;

import com.wavefront.api.DataIngesterAPI;
import com.wavefront.common.NamedThreadFactory;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Wavefront Client that sends data directly to Wavefront via the direct ingestion APIs.
 *
 * @author Vikram Raman (vikram@wavefront.com)
 */
public class WavefrontDirectSender implements WavefrontSender, Runnable {

  private static final String DEFAULT_SOURCE = "wavefrontDirectSender";
  private static final Logger LOGGER = LoggerFactory.getLogger(WavefrontDirectSender.class);
  private static final String quote = "\"";
  private static final String escapedQuote = "\\\"";
  private static final int MAX_QUEUE_SIZE = 50000;
  private static final int BATCH_SIZE = 10000;

  private ScheduledExecutorService scheduler;
  private final String server;
  private final String token;
  private DataIngesterAPI directService;
  private final LinkedBlockingQueue<String> buffer = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

  public WavefrontDirectSender(String server, String token) {
    this.server = server;
    this.token = token;
  }

  @Override
  public synchronized void connect() throws IllegalStateException, IOException {
    if (directService == null) {
      directService = new DataIngesterService(server, token);
      scheduler = Executors.newScheduledThreadPool(1, new NamedThreadFactory(DEFAULT_SOURCE));
      scheduler.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
    }
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

  private void addPoint(@NotNull String name, double value, @Nullable Long timestamp, @NotNull String source,
                        @Nullable Map<String, String> pointTags) throws IOException {
    String point = pointToString(name, value, timestamp, source, pointTags);
    if (point != null && !buffer.offer(point)) {
      LOGGER.debug("Buffer full, dropping point " + name);
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
  public void flush() throws IOException {
    internalFlush();
  }

  private void internalFlush() throws IOException {

    if (!isConnected()) {
        return;
    }

    List<String> points = getPointsBatch();
    if (points.isEmpty()) {
      return;
    }

    Response response = null;
    try (InputStream is = pointsToStream(points)) {
      response = directService.report("graphite_v2", is);
      if (response.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR ||
          response.getStatusInfo().getFamily() == Response.Status.Family.CLIENT_ERROR) {
        LOGGER.debug("Error reporting points, respStatus=" + response.getStatus());
        try {
          buffer.addAll(points);
        } catch (Exception ex) {
          // unlike offer(), addAll adds partially and throws an exception if buffer full
          LOGGER.debug("Buffer full, dropping attempted points");
        }
      }
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

  private List<String> getPointsBatch() {
    int blockSize = Math.min(buffer.size(), BATCH_SIZE);
    List<String> points = new ArrayList<>(blockSize);
    buffer.drainTo(points, blockSize);
    return points;
  }

  @Override
  public synchronized boolean isConnected() {
    return directService != null;
  }

  @Override
  public int getFailureCount() {
    return 0;
  }

  @Override
  public synchronized void close() throws IOException {
    if (directService != null) {
      try {
        scheduler.shutdownNow();
      } catch (SecurityException ex) {
        LOGGER.debug("shutdown error", ex);
      }
      scheduler = null;
      directService = null;
    }
  }

  @Override
  public void run() {
    try {
      this.internalFlush();
    } catch (Throwable ex) {
      LOGGER.debug("Unable to report to Wavefront", ex);
    }
  }

  private static final class DataIngesterService implements DataIngesterAPI {

    private final String token;
    private final URI uri;
    private static final String BAD_REQUEST = "Bad client request";
    private static final int CONNECT_TIMEOUT = 30000;
    private static final int READ_TIMEOUT = 10000;

    public DataIngesterService(String server, String token) throws IOException  {
      this.token = token;
      uri = URI.create(server);
    }

    @Override
    public Response report(String format, InputStream stream) throws IOException {

      /**
       * Refer https://docs.oracle.com/javase/8/docs/technotes/guides/net/http-keepalive.html
       * for details around why this code is written as it is.
       */

      int statusCode = 400;
      String respMsg = BAD_REQUEST;
      HttpURLConnection urlConn = null;
      try {
        URL url = new URL(uri.getScheme(), uri.getHost(), uri.getPort(), String.format("/report?f=" + format));
        urlConn = (HttpURLConnection) url.openConnection();
        urlConn.setDoOutput(true);
        urlConn.addRequestProperty(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM);
        urlConn.addRequestProperty(HttpHeaders.CONTENT_ENCODING, "gzip");
        urlConn.addRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + token);

        urlConn.setConnectTimeout(CONNECT_TIMEOUT);
        urlConn.setReadTimeout(READ_TIMEOUT);

        try (GZIPOutputStream gzipOS = new GZIPOutputStream(urlConn.getOutputStream())) {
          byte[] buffer = new byte[4096];
          int len = 0;
          while ((len = stream.read(buffer)) > 0) {
            gzipOS.write(buffer);
          }
          gzipOS.flush();
        }
        statusCode = urlConn.getResponseCode();
        respMsg = urlConn.getResponseMessage();
        readAndClose(urlConn.getInputStream());
      } catch (IOException ex) {
        if (urlConn != null) {
          statusCode = urlConn.getResponseCode();
          respMsg = urlConn.getResponseMessage();
          readAndClose(urlConn.getErrorStream());
        }
      }
      return Response.status(statusCode).entity(respMsg).build();
    }

    private void readAndClose(InputStream stream) throws IOException {
      if (stream != null) {
        try (InputStream is = stream) {
          byte[] buffer = new byte[4096];
          int ret = 0;
          // read entire stream before closing
          while ((ret = is.read(buffer)) > 0) {}
        }
      }
    }
  }
}
