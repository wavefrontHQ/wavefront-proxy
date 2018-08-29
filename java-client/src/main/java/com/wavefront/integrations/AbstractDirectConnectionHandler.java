package com.wavefront.integrations;

import com.wavefront.api.DataIngesterAPI;
import com.wavefront.common.NamedThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Abstract base class for sending data directly to a Wavefront service.
 *
 * @author Vikram Raman (vikram@wavefront.com)
 */
public abstract class AbstractDirectConnectionHandler implements WavefrontConnectionHandler, Runnable {

  private static final String DEFAULT_SOURCE = "wavefrontDirectSender";
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDirectConnectionHandler.class);

  private ScheduledExecutorService scheduler;
  private final String server;
  private final String token;
  private DataIngesterAPI directService;

  protected AbstractDirectConnectionHandler(String server, String token) {
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
  public void flush() throws IOException {
    internalFlush();
  }

  protected abstract void internalFlush() throws IOException;

  @Override
  public synchronized boolean isConnected() {
    return directService != null;
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

  protected Response report(String format, InputStream is) throws IOException {
    return directService.report(format, is);
  }

  private static final class DataIngesterService implements DataIngesterAPI {

    private final String token;
    private final URI uri;
    private static final String BAD_REQUEST = "Bad client request";
    private static final int CONNECT_TIMEOUT = 30000;
    private static final int READ_TIMEOUT = 10000;

    public DataIngesterService(String server, String token) {
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
