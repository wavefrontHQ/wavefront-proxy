package com.wavefront.agent;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.wavefront.agent.data.EntityRateLimiter;
import com.wavefront.ingester.SpanDecoder;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;
import javax.net.SocketFactory;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.message.BasicHeader;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wavefront.report.Span;

public class TestUtils {
  private static final Logger logger = LoggerFactory.getLogger(TestUtils.class.getCanonicalName());

  public static <T extends HttpRequestBase> T httpEq(HttpRequestBase request) {
    EasyMock.reportMatcher(
        new IArgumentMatcher() {
          @Override
          public boolean matches(Object o) {
            return o instanceof HttpRequestBase
                && o.getClass().getCanonicalName().equals(request.getClass().getCanonicalName())
                && ((HttpRequestBase) o).getMethod().equals(request.getMethod())
                && ((HttpRequestBase) o).getProtocolVersion().equals(request.getProtocolVersion())
                && ((HttpRequestBase) o).getURI().equals(request.getURI());
          }

          @Override
          public void appendTo(StringBuffer stringBuffer) {
            stringBuffer.append("httpEq(");
            stringBuffer.append(request.toString());
            stringBuffer.append(")");
          }
        });
    return null;
  }

  public static void expectHttpResponse(
      HttpClient httpClient, HttpRequestBase req, byte[] content, int httpStatus) throws Exception {
    HttpResponse response = EasyMock.createMock(HttpResponse.class);
    HttpEntity entity = EasyMock.createMock(HttpEntity.class);
    StatusLine line = EasyMock.createMock(StatusLine.class);

    EasyMock.expect(response.getStatusLine()).andReturn(line).anyTimes();
    EasyMock.expect(response.getEntity()).andReturn(entity).anyTimes();
    EasyMock.expect(line.getStatusCode()).andReturn(httpStatus).anyTimes();
    EasyMock.expect(line.getReasonPhrase()).andReturn("OK").anyTimes();
    EasyMock.expect(entity.getContent()).andReturn(new ByteArrayInputStream(content)).anyTimes();
    EasyMock.expect(entity.getContentLength()).andReturn((long) content.length).atLeastOnce();
    EasyMock.expect(entity.getContentType())
        .andReturn(new BasicHeader("Content-Type", "application/json"))
        .anyTimes();

    EasyMock.expect(httpClient.execute(httpEq(req))).andReturn(response).once();

    EasyMock.replay(httpClient, response, entity, line);
  }

  public static int findAvailablePort() {
    try {
      ServerSocket socket = new ServerSocket(0);
      int portNum = socket.getLocalPort();
      socket.close();
      logger.info("Found available port: " + portNum);
      return portNum;
    } catch (IOException exc) {
      throw new RuntimeException(exc);
    }
  }

  public static void waitUntilListenerIsOnline(int port) throws Exception {
    final int maxTries = 100;
    for (int i = 0; i < maxTries; i++) {
      try {
        Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
        socket.close();
        return;
      } catch (IOException exc) {
        TimeUnit.MILLISECONDS.sleep(50);
      }
    }
    throw new RuntimeException(
        "Giving up connecting to port " + port + " after " + maxTries + " tries.");
  }

  public static int gzippedHttpPost(String postUrl, String payload) throws Exception {
    URL url = new URL(postUrl);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setDoOutput(true);
    connection.setDoInput(true);
    connection.setRequestProperty("Content-Encoding", "gzip");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(baos);
    gzip.write(payload.getBytes(StandardCharsets.UTF_8));
    gzip.close();
    connection.getOutputStream().write(baos.toByteArray());
    connection.getOutputStream().flush();
    int response = connection.getResponseCode();
    logger.info("HTTP response code (gzipped content): " + response);
    return response;
  }

  public static int httpGet(String urlGet) throws Exception {
    URL url = new URL(urlGet);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    connection.setDoOutput(false);
    connection.setDoInput(true);
    int response = connection.getResponseCode();
    logger.info("HTTP GET response code: " + response);
    return response;
  }

  public static int httpPost(String urlGet, String payload) throws Exception {
    URL url = new URL(urlGet);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setDoOutput(true);
    connection.setDoInput(true);
    BufferedWriter writer =
        new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));
    writer.write(payload);
    writer.flush();
    writer.close();
    int response = connection.getResponseCode();
    logger.info("HTTP POST response code (plaintext content): " + response);
    return response;
  }

  public static int httpPost(String urlGet, byte[] payload, String mediaType) throws Exception {
    URL url = new URL(urlGet);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("content-type", mediaType);
    connection.setDoOutput(true);
    connection.setDoInput(true);

    DataOutputStream writer = new DataOutputStream(connection.getOutputStream());
    writer.write(payload);
    writer.flush();
    writer.close();
    int response = connection.getResponseCode();
    logger.info("HTTP POST response code (plaintext content): " + response);
    return response;
  }

  public static String getResource(String resourceName) throws Exception {
    URL url = Resources.getResource("com.wavefront.agent/" + resourceName);
    File myFile = new File(url.toURI());
    return FileUtils.readFileToString(myFile, "UTF-8");
  }

  /**
   * Verify mocks with retries until specified timeout expires. A healthier alternative to putting
   * Thread.sleep() before verify().
   *
   * @param timeout Desired timeout in milliseconds
   * @param mocks Mock objects to verify (sequentially).
   */
  public static void verifyWithTimeout(int timeout, Object... mocks) {
    int sleepIntervalMillis = 10;
    for (Object mock : mocks) {
      int millisLeft = timeout;
      while (true) {
        try {
          EasyMock.verify(mock);
          break;
        } catch (AssertionError e) {
          if (millisLeft <= 0) {
            logger.warn("verify() failed after : " + (timeout - millisLeft) + "ms");
            throw e;
          }
          try {
            TimeUnit.MILLISECONDS.sleep(sleepIntervalMillis);
          } catch (InterruptedException x) {
            //
          }
          millisLeft -= sleepIntervalMillis;
        }
      }
      long waitTime = timeout - millisLeft;
      if (waitTime > 0) {
        logger.info("verify() wait time: " + waitTime + "ms");
      }
    }
  }

  public static void assertTrueWithTimeout(int timeout, Supplier<Boolean> assertion) {
    int sleepIntervalMillis = 10;
    int millisLeft = timeout;
    while (true) {
      if (assertion.get()) break;
      if (millisLeft <= 0)
        throw new AssertionError("Assertion timed out (" + (timeout - millisLeft) + "ms)");
      try {
        TimeUnit.MILLISECONDS.sleep(sleepIntervalMillis);
      } catch (InterruptedException x) {
        //
      }
      millisLeft -= sleepIntervalMillis;
    }
    long waitTime = timeout - millisLeft;
    if (waitTime > 0) {
      logger.info("assertTrueWithTimeout() wait time: " + waitTime + "ms");
    }
  }

  public static Span parseSpan(String line) {
    List<Span> out = Lists.newArrayListWithExpectedSize(1);
    new SpanDecoder("unknown").decode(line, out, "dummy");
    return out.get(0);
  }

  public static class RateLimiter extends EntityRateLimiter {
    @Override
    public boolean tryAcquire(int points) {
      return true;
    }
  }
}
