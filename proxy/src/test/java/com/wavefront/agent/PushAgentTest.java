package com.wavefront.agent;

import com.google.common.collect.ImmutableList;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import javax.net.SocketFactory;

import wavefront.report.ReportPoint;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

public class PushAgentTest {
  protected static final Logger logger = Logger.getLogger(PushAgentTest.class.getCanonicalName());

  private PushAgent proxy;
  private long startTime = System.currentTimeMillis() / 1000;
  private int port;
  private PointHandler mockPointHandler = EasyMock.createMock(PointHandlerImpl.class);

  private static int findAvailablePort(int startingPortNumber) {
    int portNum = startingPortNumber;
    ServerSocket socket;
    while (portNum < startingPortNumber + 1000) {
      try {
        socket = new ServerSocket(portNum);
        socket.close();
        return portNum;
      } catch (IOException exc) {
        // ignore
      }
      portNum++;
    }
    throw new RuntimeException("Unable to find an available port in the [" + startingPortNumber + ";" +
        (startingPortNumber + 1000) + ") range");
  }

  @Before
  public void setup() throws Exception {
    port = findAvailablePort(2878);
    proxy = new PushAgent();
    proxy.flushThreads = 2;
    proxy.retryThreads = 1;
    proxy.pushListenerPorts = String.valueOf(port);
    proxy.startGraphiteListener(proxy.pushListenerPorts, mockPointHandler, false);
    TimeUnit.MILLISECONDS.sleep(250);
  }


  @Test
  public void testWavefrontUnifiedPortHandlerPlaintextUncompressed() throws Exception {
    reset(mockPointHandler);
    mockPointHandler.reportPoints(ImmutableList.of(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test").setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build()));
    expectLastCall();
    mockPointHandler.reportPoints(ImmutableList.of(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build()));
    expectLastCall();
    replay(mockPointHandler);

    // try plaintext over tcp first
    Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr = "metric.test 0 " + startTime + " source=test1\n" +
        "metric.test 1 " + (startTime + 1) + " source=test2\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    TimeUnit.MILLISECONDS.sleep(250);
    verify(mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerGzippedPlaintextStream() throws Exception {
    reset(mockPointHandler);
    mockPointHandler.reportPoints(ImmutableList.of(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric2.test").setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build()));
    expectLastCall();
    mockPointHandler.reportPoints(ImmutableList.of(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric2.test").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build()));
    expectLastCall();
    replay(mockPointHandler);

    // try gzipped plaintext stream over tcp
    String payloadStr = "metric2.test 0 " + startTime + " source=test1\n" +
        "metric2.test 1 " + (startTime + 1) + " source=test2\n";
    Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(payloadStr.length());
    GZIPOutputStream gzip = new GZIPOutputStream(baos);
    gzip.write(payloadStr.getBytes("UTF-8"));
    gzip.close();
    socket.getOutputStream().write(baos.toByteArray());
    socket.getOutputStream().flush();
    socket.close();
    TimeUnit.MILLISECONDS.sleep(250);
    verify(mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerPlaintextOverHttp() throws Exception {
    reset(mockPointHandler);
    mockPointHandler.reportPoints(ImmutableList.of(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric3.test").setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build()));
    expectLastCall();
    mockPointHandler.reportPoints(ImmutableList.of(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric3.test").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build()));
    expectLastCall();
    mockPointHandler.reportPoints(ImmutableList.of(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric3.test").setHost("test3").setTimestamp((startTime + 2) * 1000).setValue(2.0d).build()));
    expectLastCall();
    replay(mockPointHandler);

    // try http connection
    String payloadStr = "metric3.test 0 " + startTime + " source=test1\n" +
        "metric3.test 1 " + (startTime + 1) + " source=test2\n" +
        "metric3.test 2 " + (startTime + 2) + " source=test3"; // note the lack of newline at the end!
    URL url = new URL("http://localhost:" + port);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setDoOutput(true);
    connection.setDoInput(true);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));
    writer.write(payloadStr);
    writer.flush();
    writer.close();
    logger.info("HTTP response code (plaintext content): " + connection.getResponseCode());
    TimeUnit.MILLISECONDS.sleep(250);
    verify(mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerHttpGzipped() throws Exception {
    reset(mockPointHandler);
    mockPointHandler.reportPoints(ImmutableList.of(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build()));
    expectLastCall();
    mockPointHandler.reportPoints(ImmutableList.of(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build()));
    expectLastCall();
    mockPointHandler.reportPoints(ImmutableList.of(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test3").setTimestamp((startTime + 2) * 1000).setValue(2.0d).build()));
    expectLastCall();
    replay(mockPointHandler);

    // try http connection with gzip
    String payloadStr = "metric4.test 0 " + startTime + " source=test1\n" +
        "metric4.test 1 " + (startTime + 1) + " source=test2\n" +
        "metric4.test 2 " + (startTime + 2) + " source=test3"; // note the lack of newline at the end!
    URL url = new URL("http://localhost:" + port);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setDoOutput(true);
    connection.setDoInput(true);
    connection.setRequestProperty("Content-Encoding", "gzip");
    ByteArrayOutputStream baos = new ByteArrayOutputStream(payloadStr.length());
    GZIPOutputStream gzip = new GZIPOutputStream(baos);
    gzip.write(payloadStr.getBytes("UTF-8"));
    gzip.close();
    connection.getOutputStream().write(baos.toByteArray());
    connection.getOutputStream().flush();
    logger.info("HTTP response code (gzipped content): " + connection.getResponseCode());
    TimeUnit.MILLISECONDS.sleep(250);
    verify(mockPointHandler);
  }

}
