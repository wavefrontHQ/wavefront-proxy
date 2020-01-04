package com.wavefront.agent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.auth.TokenValidationMethod;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.data.TaskQueueLevel;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.SenderTask;
import com.wavefront.agent.handlers.SenderTaskFactory;
import com.wavefront.sdk.entities.tracing.sampling.RateSampler;
import junit.framework.AssertionFailedError;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.Histogram;
import wavefront.report.HistogramType;
import wavefront.report.ReportPoint;
import wavefront.report.ReportSourceTag;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

import javax.annotation.Nonnull;
import javax.net.SocketFactory;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import static com.wavefront.agent.TestUtils.findAvailablePort;
import static com.wavefront.agent.TestUtils.getResource;
import static com.wavefront.agent.TestUtils.gzippedHttpPost;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@NotThreadSafe
public class PushAgentTest {
  protected static final Logger logger = Logger.getLogger(PushAgentTest.class.getCanonicalName());

  private PushAgent proxy;
  private long startTime = System.currentTimeMillis() / 1000 / 60 * 60;
  private int port;
  private int tracePort;
  private int ddPort;
  private int deltaPort;
  private ReportableEntityHandler<ReportPoint, String> mockPointHandler =
      MockReportableEntityHandlerFactory.getMockReportPointHandler();
  private ReportableEntityHandler<ReportSourceTag, ReportSourceTag> mockSourceTagHandler =
      MockReportableEntityHandlerFactory.getMockSourceTagHandler();
  private ReportableEntityHandler<ReportPoint, String> mockHistogramHandler =
      MockReportableEntityHandlerFactory.getMockHistogramHandler();
  private ReportableEntityHandler<Span, String> mockTraceHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private ReportableEntityHandler<SpanLogs, String> mockTraceSpanLogsHandler =
      MockReportableEntityHandlerFactory.getMockTraceSpanLogsHandler();
  private SenderTask<String> mockSenderTask = EasyMock.createNiceMock(SenderTask.class);
  private Collection<SenderTask<String>> mockSenderTasks = ImmutableList.of(mockSenderTask);
  private SenderTaskFactory mockSenderTaskFactory = new SenderTaskFactory() {
    @SuppressWarnings("unchecked")
    @Override
    public Collection<SenderTask<String>> createSenderTasks(@NotNull HandlerKey handlerKey, int numThreads) {
      return mockSenderTasks;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void shutdown(@Nonnull String handle) {
    }

    @Override
    public void drainBuffersToQueue(QueueingReason reason) {
    }
  };

  private ReportableEntityHandlerFactory mockHandlerFactory =
      MockReportableEntityHandlerFactory.createMockHandlerFactory(mockPointHandler,
          mockSourceTagHandler, mockHistogramHandler, mockTraceHandler,
          mockTraceSpanLogsHandler);
  private HttpClient mockHttpClient = EasyMock.createMock(HttpClient.class);

  @Before
  public void setup() throws Exception {
    port = findAvailablePort(2888);
    tracePort = findAvailablePort(3888);
    ddPort = findAvailablePort(4888);
    deltaPort = findAvailablePort(5888);
    proxy = new PushAgent();
    proxy.proxyConfig.flushThreads = 2;
    proxy.proxyConfig.dataBackfillCutoffHours = 100000000;
    proxy.proxyConfig.pushListenerPorts = String.valueOf(port);
    proxy.proxyConfig.deltaCountersAggregationListenerPorts = String.valueOf(deltaPort);
    proxy.proxyConfig.traceListenerPorts = String.valueOf(tracePort);
    proxy.proxyConfig.dataDogJsonPorts = String.valueOf(ddPort);
    proxy.proxyConfig.dataDogProcessSystemMetrics = false;
    proxy.proxyConfig.dataDogProcessServiceChecks = true;
    proxy.proxyConfig.deltaCountersAggregationIntervalSeconds = 3;
    assertEquals(Integer.valueOf(2), proxy.proxyConfig.getFlushThreads());
    assertFalse(proxy.proxyConfig.isDataDogProcessSystemMetrics());
    assertTrue(proxy.proxyConfig.isDataDogProcessServiceChecks());
    proxy.startGraphiteListener(proxy.proxyConfig.getPushListenerPorts(), mockHandlerFactory, null);
    proxy.startDeltaCounterListener(proxy.proxyConfig.getDeltaCountersAggregationListenerPorts(),
        null, mockSenderTaskFactory);
    proxy.startTraceListener(proxy.proxyConfig.getTraceListenerPorts(), mockHandlerFactory,
        new RateSampler(1.0D));
    proxy.startDataDogListener(proxy.proxyConfig.getDataDogJsonPorts(), mockHandlerFactory,
        mockHttpClient);
    TimeUnit.MILLISECONDS.sleep(500);
  }

  @After
  public void teardown() {
    proxy.shutdown();
  }

  @Test
  public void testWavefrontUnifiedPortHandlerPlaintextUncompressed() throws Exception {
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test").setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build());
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
    TimeUnit.MILLISECONDS.sleep(500);
    verify(mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerGzippedPlaintextStream() throws Exception {
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric2.test").setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric2.test").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build());
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
    TimeUnit.MILLISECONDS.sleep(500);
    verify(mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerPlaintextOverHttp() throws Exception {
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric3.test").setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric3.test").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric3.test").setHost("test3").setTimestamp((startTime + 2) * 1000).setValue(2.0d).build());
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
    verify(mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerHttpGzipped() throws Exception {
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test3").setTimestamp((startTime + 2) * 1000).setValue(2.0d).build());
    expectLastCall();
    replay(mockPointHandler);

    // try http connection with gzip
    String payloadStr = "metric4.test 0 " + startTime + " source=test1\n" +
        "metric4.test 1 " + (startTime + 1) + " source=test2\n" +
        "metric4.test 2 " + (startTime + 2) + " source=test3"; // note the lack of newline at the end!
    gzippedHttpPost("http://localhost:" + port, payloadStr);
    verify(mockPointHandler);
  }

  // test that histograms received on Wavefront port get routed to the correct handler
  @Test
  public void testHistogramDataOnWavefrontUnifiedPortHandlerPlaintextUncompressed() throws Exception {
    reset(mockHistogramHandler);
    mockHistogramHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.histo").setHost("test1").setTimestamp(startTime * 1000).setValue(
        Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setDuration(60000)
            .setBins(ImmutableList.of(10.0d, 100.0d))
            .setCounts(ImmutableList.of(5, 10))
            .build())
        .build());
    mockHistogramHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.histo").setHost("test2").setTimestamp((startTime + 60) * 1000).setValue(
        Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setDuration(60000)
            .setBins(ImmutableList.of(20.0d, 30.0d, 40.0d))
            .setCounts(ImmutableList.of(5, 6, 7))
            .build())
        .build());
    expectLastCall();
    replay(mockHistogramHandler);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr = "!M " + startTime + " #5 10.0 #10 100.0 metric.test.histo source=test1\n" +
        "!M " + (startTime + 60) + " #5 20.0 #6 30.0 #7 40.0 metric.test.histo source=test2\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    TimeUnit.MILLISECONDS.sleep(500);
    verify(mockHistogramHandler);
  }

  // test Wavefront port handler with mixed payload: metrics, histograms, source tags
  @Test
  public void testWavefrontUnifiedPortHandlerPlaintextUncompressedMixedDataPayload() throws Exception {
    reset(mockHistogramHandler);
    reset(mockPointHandler);
    reset(mockSourceTagHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.mixed").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(10d).build());
    mockHistogramHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.mixed").setHost("test1").setTimestamp(startTime * 1000).setValue(
        Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setDuration(60000)
            .setBins(ImmutableList.of(10.0d, 100.0d))
            .setCounts(ImmutableList.of(5, 10))
            .build())
        .build());
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.mixed").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(9d).build());
    mockSourceTagHandler.report(ReportSourceTag.newBuilder().setSourceTagLiteral("SourceTag").setAction("save")
        .setSource("testSource").setAnnotations(ImmutableList.of("newtag1", "newtag2")).setDescription("").build());
    expectLastCall();
    replay(mockPointHandler);
    replay(mockHistogramHandler);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr = "metric.test.mixed 10.0 " + (startTime + 1) + " source=test2\n" +
        "!M " + startTime + " #5 10.0 #10 100.0 metric.test.mixed source=test1\n" +
        "@SourceTag action=save source=testSource newtag1 newtag2\n" +
        "metric.test.mixed 9.0 " + (startTime + 1) + " source=test2\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    TimeUnit.MILLISECONDS.sleep(500);
    verify(mockPointHandler);
    verify(mockHistogramHandler);
  }

  @Test
  public void testTraceUnifiedPortHandlerPlaintext() throws Exception {
    reset(mockTraceHandler);
    reset(mockTraceSpanLogsHandler);
    String traceId = UUID.randomUUID().toString();
    long timestamp1 = startTime * 1000000 + 12345;
    long timestamp2 = startTime * 1000000 + 23456;
    mockTraceSpanLogsHandler.report(SpanLogs.newBuilder().
        setCustomer("dummy").
        setTraceId(traceId).
        setSpanId("testspanid").
        setLogs(ImmutableList.of(
            SpanLog.newBuilder().
                setTimestamp(timestamp1).
                setFields(ImmutableMap.of("key", "value", "key2", "value2")).
                build(),
            SpanLog.newBuilder().
                setTimestamp(timestamp2).
                setFields(ImmutableMap.of("key3", "value3", "key4", "value4")).
                build()
        )).
        build());
    expectLastCall();
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime * 1000)
        .setDuration(1000)
        .setName("testSpanName")
        .setSource("testsource")
        .setSpanId("testspanid")
        .setTraceId(traceId)
        .setAnnotations(ImmutableList.of(new Annotation("parent", "parent1"), new Annotation("parent", "parent2")))
        .build());
    expectLastCall();
    replay(mockTraceHandler);
    replay(mockTraceSpanLogsHandler);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", tracePort);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr = "testSpanName parent=parent1 source=testsource spanId=testspanid " +
        "traceId=\"" + traceId + "\" parent=parent2 " + startTime + " " + (startTime + 1) + "\n" +
        "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId + "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" + timestamp2 +
        ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    TimeUnit.MILLISECONDS.sleep(500);
    verify(mockTraceHandler);
    verify(mockTraceSpanLogsHandler);
  }

  @Test(timeout = 30000)
  public void testDataDogUnifiedPortHandler() throws Exception {
    int ddPort2 = findAvailablePort(4988);
    PushAgent proxy2 = new PushAgent();
    proxy2.proxyConfig.flushThreads = 2;
    proxy2.proxyConfig.dataBackfillCutoffHours = 100000000;
    proxy2.proxyConfig.dataDogJsonPorts = String.valueOf(ddPort2);
    proxy2.proxyConfig.dataDogProcessSystemMetrics = true;
    proxy2.proxyConfig.dataDogProcessServiceChecks = false;
    proxy2.proxyConfig.dataDogRequestRelayTarget = "http://relay-to:1234";
    assertEquals(Integer.valueOf(2), proxy2.proxyConfig.getFlushThreads());
    assertTrue(proxy2.proxyConfig.isDataDogProcessSystemMetrics());
    assertFalse(proxy2.proxyConfig.isDataDogProcessServiceChecks());

    proxy2.startDataDogListener(proxy2.proxyConfig.getDataDogJsonPorts(), mockHandlerFactory,
        mockHttpClient);
    TimeUnit.MILLISECONDS.sleep(500);

    // test 1: post to /intake with system metrics enabled and http relay enabled
    HttpResponse mockHttpResponse = EasyMock.createMock(HttpResponse.class);
    StatusLine mockStatusLine = EasyMock.createMock(StatusLine.class);
    reset(mockPointHandler, mockHttpClient, mockHttpResponse, mockStatusLine);
    expect(mockStatusLine.getStatusCode()).andReturn(200);
    expect(mockHttpResponse.getStatusLine()).andReturn(mockStatusLine);
    expect(mockHttpResponse.getEntity()).andReturn(new StringEntity(""));
    expect(mockHttpClient.execute(anyObject(HttpPost.class))).andReturn(mockHttpResponse);
    mockPointHandler.report(anyObject());
    expectLastCall().times(46);
    replay(mockHttpClient, mockHttpResponse, mockStatusLine, mockPointHandler);
    gzippedHttpPost("http://localhost:" + ddPort2 + "/intake", getResource("ddTestSystem.json"));
    verify(mockHttpClient, mockPointHandler);

    // test 2: post to /intake with system metrics disabled and http relay disabled
    reset(mockPointHandler);
    mockPointHandler.report(anyObject());
    expectLastCall().andThrow(new AssertionFailedError()).anyTimes();
    replay(mockPointHandler);
    gzippedHttpPost("http://localhost:" + ddPort + "/intake", getResource("ddTestSystem.json"));
    verify(mockPointHandler);

    // test 3: post to /intake with system metrics enabled and http relay enabled, but remote unavailable
    reset(mockPointHandler, mockHttpClient, mockHttpResponse, mockStatusLine);
    expect(mockStatusLine.getStatusCode()).andReturn(404); // remote returns a error http code
    expect(mockHttpResponse.getStatusLine()).andReturn(mockStatusLine);
    expect(mockHttpResponse.getEntity()).andReturn(new StringEntity(""));
    expect(mockHttpClient.execute(anyObject(HttpPost.class))).andReturn(mockHttpResponse);
    mockPointHandler.report(anyObject());
    expectLastCall().andThrow(new AssertionFailedError()).anyTimes(); // we are not supposed to actually process data!
    replay(mockHttpClient, mockHttpResponse, mockStatusLine, mockPointHandler);
    gzippedHttpPost("http://localhost:" + ddPort2 + "/intake", getResource("ddTestSystem.json"));
    verify(mockHttpClient, mockPointHandler);

    // test 4: post to /api/v1/check_run with service checks disabled
    reset(mockPointHandler, mockHttpClient, mockHttpResponse, mockStatusLine);
    expect(mockStatusLine.getStatusCode()).andReturn(202); // remote returns a error http code
    expect(mockHttpResponse.getStatusLine()).andReturn(mockStatusLine);
    expect(mockHttpResponse.getEntity()).andReturn(new StringEntity(""));
    expect(mockHttpClient.execute(anyObject(HttpPost.class))).andReturn(mockHttpResponse);
    mockPointHandler.report(anyObject());
    expectLastCall().andThrow(new AssertionFailedError()).anyTimes(); // we are not supposed to actually process data!
    replay(mockHttpClient, mockHttpResponse, mockStatusLine, mockPointHandler);
    gzippedHttpPost("http://localhost:" + ddPort2 + "/api/v1/check_run", getResource("ddTestServiceCheck.json"));
    verify(mockHttpClient, mockPointHandler);

    // test 5: post to /api/v1/check_run with service checks enabled
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().
        setTable("dummy").
        setMetric("testApp.status").
        setHost("testhost").
        setTimestamp(1536719228000L).
        setValue(3.0d).
        build());
    expectLastCall().once();
    replay(mockPointHandler);
    gzippedHttpPost("http://localhost:" + ddPort + "/api/v1/check_run", getResource("ddTestServiceCheck.json"));
    verify(mockPointHandler);

    // test 6: post to /api/v1/series
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().
        setTable("dummy").
        setMetric("system.net.tcp.retrans_segs").
        setHost("testhost").
        setTimestamp(1531176936000L).
        setValue(0.0d).
        build());
    expectLastCall().once();
    mockPointHandler.report(ReportPoint.newBuilder().
        setTable("dummy").
        setMetric("system.net.tcp.listen_drops").
        setHost("testhost").
        setTimestamp(1531176936000L).
        setValue(0.0d).
        build());
    expectLastCall().once();
    mockPointHandler.report(ReportPoint.newBuilder().
        setTable("dummy").
        setMetric("system.net.packets_in.count").
        setHost("testhost").
        setTimestamp(1531176936000L).
        setValue(12.052631578947368d).
        build());
    expectLastCall().once();
    replay(mockPointHandler);
    gzippedHttpPost("http://localhost:" + ddPort + "/api/v1/series", getResource("ddTestTimeseries.json"));
    verify(mockPointHandler);

    // test 7: post multiple checks to /api/v1/check_run with service checks enabled
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().
        setTable("dummy").
        setMetric("testApp.status").
        setHost("testhost").
        setTimestamp(1536719228000L).
        setValue(3.0d).
        build());
    expectLastCall().once();
    mockPointHandler.report(ReportPoint.newBuilder().
        setTable("dummy").
        setMetric("testApp2.status").
        setHost("testhost2").
        setTimestamp(1536719228000L).
        setValue(2.0d).
        build());
    expectLastCall().once();
    replay(mockPointHandler);
    gzippedHttpPost("http://localhost:" + ddPort + "/api/v1/check_run",
        getResource("ddTestMultipleServiceChecks.json"));
    verify(mockPointHandler);

  }

  @Test
  public void testDeltaCounterHandlerMixedData() throws Exception {
    reset(mockSenderTask);
    Capture<String> capturedArgument = Capture.newInstance(CaptureType.ALL);
    mockSenderTask.add(EasyMock.capture(capturedArgument));
    expectLastCall().atLeastOnce();
    replay(mockSenderTask);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", deltaPort);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr1 = "∆test.mixed1 1.0 source=test1\n";
    String payloadStr2 = "∆test.mixed2 2.0 source=test1\n";
    String payloadStr3 = "test.mixed3 3.0 source=test1\n";
    String payloadStr4 = "∆test.mixed3 3.0 source=test1\n";
    stream.write(payloadStr1.getBytes());
    stream.write(payloadStr2.getBytes());
    stream.write(payloadStr3.getBytes());
    stream.write(payloadStr4.getBytes());
    stream.flush();
    TimeUnit.MILLISECONDS.sleep(10000);
    socket.close();
    verify(mockSenderTask);
    String[] reportPoints = { "1.0", "2.0", "3.0" };
    int pointInd = 0;
    for (String s : capturedArgument.getValues()) {
      System.out.println(s);
      assertTrue(s.startsWith("\"∆test.mixed" + Integer.toString(pointInd + 1) + "\" " +
          reportPoints[pointInd]));
      pointInd += 1;
    }
  }

  @Test
  public void testDeltaCounterHandlerDataStream() throws Exception {
    reset(mockSenderTask);
    Capture<String> capturedArgument = Capture.newInstance(CaptureType.ALL);
    mockSenderTask.add(EasyMock.capture(capturedArgument));
    expectLastCall().atLeastOnce();
    replay(mockSenderTask);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", deltaPort);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr = "∆test.mixed 1.0 " + startTime + " source=test1\n";
    stream.write(payloadStr.getBytes());
    stream.write(payloadStr.getBytes());
    stream.flush();
    TimeUnit.MILLISECONDS.sleep(6000);
    stream.write(payloadStr.getBytes());
    stream.flush();
    TimeUnit.MILLISECONDS.sleep(1000);
    stream.write(payloadStr.getBytes());
    stream.write(payloadStr.getBytes());
    stream.flush();
    TimeUnit.MILLISECONDS.sleep(6000);

    socket.close();
    verify(mockSenderTask);
    String[] reportPoints = { "2.0", "3.0" };
    int pointInd = 0;
    for (String s : capturedArgument.getValues()) {
      assertTrue(s.startsWith("\"∆test.mixed\" " + reportPoints[pointInd]));
      pointInd += 1;
    }
  }

}