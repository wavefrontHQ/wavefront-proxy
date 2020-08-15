package com.wavefront.agent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.wavefront.agent.channel.HealthCheckManagerImpl;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.handlers.DeltaCounterAccumulationHandlerImpl;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.SenderTask;
import com.wavefront.agent.handlers.SenderTaskFactory;
import com.wavefront.agent.preprocessor.PreprocessorRuleMetrics;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.preprocessor.SpanAddAnnotationIfNotExistsTransformer;
import com.wavefront.agent.preprocessor.SpanReplaceRegexTransformer;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.agent.tls.NaiveTrustManager;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.Event;
import com.wavefront.dto.SourceTag;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.entities.tracing.sampling.DurationSampler;
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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import javax.annotation.Nonnull;
import javax.net.SocketFactory;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

import wavefront.report.Annotation;
import wavefront.report.Histogram;
import wavefront.report.HistogramType;
import wavefront.report.ReportEvent;
import wavefront.report.ReportPoint;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;
import wavefront.report.SourceTagAction;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

import static com.wavefront.agent.TestUtils.findAvailablePort;
import static com.wavefront.agent.TestUtils.getResource;
import static com.wavefront.agent.TestUtils.gzippedHttpPost;
import static com.wavefront.agent.TestUtils.httpGet;
import static com.wavefront.agent.TestUtils.httpPost;
import static com.wavefront.agent.TestUtils.verifyWithTimeout;
import static com.wavefront.agent.TestUtils.waitUntilListenerIsOnline;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.HEART_BEAT_METRIC;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.startsWith;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@NotThreadSafe
public class PushAgentTest {
  protected static final Logger logger = Logger.getLogger(PushAgentTest.class.getCanonicalName());

  private PushAgent proxy;
  private long startTime = System.currentTimeMillis() / 1000 / 60 * 60;
  private int port;
  private int tracePort;
  private int customTracePort;
  private int ddPort;
  private int deltaPort;
  private static SSLSocketFactory sslSocketFactory;
  private ReportableEntityHandler<ReportPoint, String> mockPointHandler =
      MockReportableEntityHandlerFactory.getMockReportPointHandler();
  private ReportableEntityHandler<ReportSourceTag, SourceTag> mockSourceTagHandler =
      MockReportableEntityHandlerFactory.getMockSourceTagHandler();
  private ReportableEntityHandler<ReportPoint, String> mockHistogramHandler =
      MockReportableEntityHandlerFactory.getMockHistogramHandler();
  private ReportableEntityHandler<Span, String> mockTraceHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private ReportableEntityHandler<SpanLogs, String> mockTraceSpanLogsHandler =
      MockReportableEntityHandlerFactory.getMockTraceSpanLogsHandler();
  private ReportableEntityHandler<ReportEvent, Event> mockEventHandler =
      MockReportableEntityHandlerFactory.getMockEventHandlerImpl();
  private WavefrontSender mockWavefrontSender = EasyMock.createMock(WavefrontSender.class);
  private SenderTask<String> mockSenderTask = EasyMock.createNiceMock(SenderTask.class);
  private Collection<SenderTask<String>> mockSenderTasks = ImmutableList.of(mockSenderTask);

  // Derived RED metrics related.
  private final String PREPROCESSED_APPLICATION_TAG_VALUE = "preprocessedApplication";
  private final String PREPROCESSED_SERVICE_TAG_VALUE = "preprocessedService";
  private final String PREPROCESSED_CLUSTER_TAG_VALUE = "preprocessedCluster";
  private final String PREPROCESSED_SHARD_TAG_VALUE = "preprocessedShard";
  private final String PREPROCESSED_SOURCE_VALUE = "preprocessedSource";

  private SenderTaskFactory mockSenderTaskFactory = new SenderTaskFactory() {
    @SuppressWarnings("unchecked")
    @Override
    public Collection<SenderTask<String>> createSenderTasks(@Nonnull HandlerKey handlerKey) {
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
          mockTraceSpanLogsHandler, mockEventHandler);
  private HttpClient mockHttpClient = EasyMock.createMock(HttpClient.class);

  @BeforeClass
  public static void init() throws Exception {
    TrustManager[] tm = new TrustManager[]{new NaiveTrustManager()};
    SSLContext context = SSLContext.getInstance("SSL");
    context.init(new KeyManager[0], tm, new SecureRandom());
    sslSocketFactory = context.getSocketFactory();
    HttpsURLConnection.setDefaultSSLSocketFactory(context.getSocketFactory());
    HttpsURLConnection.setDefaultHostnameVerifier((h, s) -> h.equals("localhost"));
  }

  @Before
  public void setup() throws Exception {
    proxy = new PushAgent();
    proxy.proxyConfig.flushThreads = 2;
    proxy.proxyConfig.dataBackfillCutoffHours = 100000000;
    proxy.proxyConfig.dataDogRequestRelaySyncMode = true;
    proxy.proxyConfig.dataDogProcessSystemMetrics = false;
    proxy.proxyConfig.dataDogProcessServiceChecks = true;
    assertEquals(Integer.valueOf(2), proxy.proxyConfig.getFlushThreads());
    assertFalse(proxy.proxyConfig.isDataDogProcessSystemMetrics());
    assertTrue(proxy.proxyConfig.isDataDogProcessServiceChecks());
  }

  @After
  public void teardown() {
    proxy.shutdown();
  }

  @Test
  public void testSecureAll() throws Exception {
    int securePort1 = findAvailablePort(2888);
    int securePort2 = findAvailablePort(2889);
    proxy.proxyConfig.privateCertPath = getClass().getClassLoader().getResource("demo.cert").getPath();
    proxy.proxyConfig.privateKeyPath = getClass().getClassLoader().getResource("demo.key").getPath();
    proxy.proxyConfig.tlsPorts = "*";
    proxy.initSslContext();
    proxy.proxyConfig.pushListenerPorts = securePort1 + "," + securePort2;
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D),
        proxy.proxyConfig.isTraceAlwaysSampleErrors());
    proxy.startGraphiteListener(String.valueOf(securePort1), mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(String.valueOf(securePort2), mockHandlerFactory, null, sampler);
    waitUntilListenerIsOnline(securePort1);
    waitUntilListenerIsOnline(securePort2);
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric.test").setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric.test").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build());
    expectLastCall();
    replay(mockPointHandler);

    // try plaintext over tcp first
    Socket socket = sslSocketFactory.createSocket("localhost", securePort1);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr = "metric.test 0 " + startTime + " source=test1\n" +
            "metric.test 1 " + (startTime + 1) + " source=test2\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockPointHandler);

    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric.test").setHost("test3").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric.test").setHost("test4").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build());
    expectLastCall();
    replay(mockPointHandler);

    // secure test
    socket = sslSocketFactory.createSocket("localhost", securePort2);
    stream = new BufferedOutputStream(socket.getOutputStream());
    payloadStr = "metric.test 0 " + startTime + " source=test3\n" +
            "metric.test 1 " + (startTime + 1) + " source=test4\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerPlaintextUncompressed() throws Exception {
    port = findAvailablePort(2888);
    int securePort = findAvailablePort(2889);
    proxy.proxyConfig.privateCertPath = getClass().getClassLoader().getResource("demo.cert").getPath();
    proxy.proxyConfig.privateKeyPath = getClass().getClassLoader().getResource("demo.key").getPath();
    proxy.proxyConfig.tlsPorts = "1,23 , 4,   , " + securePort +"  ,6";
    proxy.initSslContext();
    proxy.proxyConfig.pushListenerPorts = port + "," + securePort;
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D),
        proxy.proxyConfig.isTraceAlwaysSampleErrors());
    proxy.startGraphiteListener(String.valueOf(port), mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(String.valueOf(securePort), mockHandlerFactory, null, sampler);
    waitUntilListenerIsOnline(port);
    waitUntilListenerIsOnline(securePort);
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
    verifyWithTimeout(500, mockPointHandler);

    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric.test").setHost("test3").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric.test").setHost("test4").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build());
    expectLastCall();
    replay(mockPointHandler);

    // secure test
    socket = sslSocketFactory.createSocket("localhost", securePort);
    stream = new BufferedOutputStream(socket.getOutputStream());
    payloadStr = "metric.test 0 " + startTime + " source=test3\n" +
            "metric.test 1 " + (startTime + 1) + " source=test4\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerGzippedPlaintextStream() throws Exception {
    port = findAvailablePort(2888);
    int securePort = findAvailablePort(2889);
    proxy.proxyConfig.privateCertPath = getClass().getClassLoader().getResource("demo.cert").getPath();
    proxy.proxyConfig.privateKeyPath = getClass().getClassLoader().getResource("demo.key").getPath();
    proxy.proxyConfig.tlsPorts = "1,23 , 4,   , " + securePort +"  ,6";
    proxy.initSslContext();
    proxy.proxyConfig.pushListenerPorts = port + "," + securePort;
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D),
        proxy.proxyConfig.isTraceAlwaysSampleErrors());
    proxy.startGraphiteListener(String.valueOf(port), mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(String.valueOf(securePort), mockHandlerFactory, null, sampler);
    waitUntilListenerIsOnline(port);
    waitUntilListenerIsOnline(securePort);
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
    verifyWithTimeout(500, mockPointHandler);

    // secure test
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric2.test").setHost("test3").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric2.test").setHost("test4").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build());
    expectLastCall();
    replay(mockPointHandler);

    // try gzipped plaintext stream over tcp
    payloadStr = "metric2.test 0 " + startTime + " source=test3\n" +
            "metric2.test 1 " + (startTime + 1) + " source=test4\n";
    socket = sslSocketFactory.createSocket("localhost", securePort);
    baos = new ByteArrayOutputStream(payloadStr.length());
    gzip = new GZIPOutputStream(baos);
    gzip.write(payloadStr.getBytes("UTF-8"));
    gzip.close();
    socket.getOutputStream().write(baos.toByteArray());
    socket.getOutputStream().flush();
    socket.close();
    verifyWithTimeout(500, mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerPlaintextOverHttp() throws Exception {
    port = findAvailablePort(2888);
    int securePort = findAvailablePort(2889);
    int healthCheckPort = findAvailablePort(8881);
    proxy.proxyConfig.privateCertPath = getClass().getClassLoader().getResource("demo.cert").getPath();
    proxy.proxyConfig.privateKeyPath = getClass().getClassLoader().getResource("demo.key").getPath();
    proxy.proxyConfig.tlsPorts = "1,23 , 4,   , " + securePort +"  ,6";
    proxy.initSslContext();
    proxy.proxyConfig.pushListenerPorts = port + "," + securePort;
    proxy.proxyConfig.httpHealthCheckPath = "/health";
    proxy.proxyConfig.httpHealthCheckPorts = String.valueOf(healthCheckPort);
    proxy.proxyConfig.httpHealthCheckAllPorts = true;
    proxy.healthCheckManager = new HealthCheckManagerImpl(proxy.proxyConfig);
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D),
        proxy.proxyConfig.isTraceAlwaysSampleErrors());
    proxy.startGraphiteListener(String.valueOf(port), mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(String.valueOf(securePort), mockHandlerFactory, null, sampler);
    proxy.startHealthCheckListener(healthCheckPort);
    waitUntilListenerIsOnline(port);
    waitUntilListenerIsOnline(securePort);
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
    assertEquals(202, httpPost("http://localhost:" + port, payloadStr));
    assertEquals(200, httpGet("http://localhost:" + port + "/health"));
    assertEquals(202, httpGet("http://localhost:" + port + "/health2"));
    assertEquals(200, httpGet("http://localhost:" + healthCheckPort + "/health"));
    assertEquals(404, httpGet("http://localhost:" + healthCheckPort + "/health2"));
    verify(mockPointHandler);

    //secure test
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric3.test").setHost("test4").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric3.test").setHost("test5").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric3.test").setHost("test6").setTimestamp((startTime + 2) * 1000).setValue(2.0d).build());
    expectLastCall();
    replay(mockPointHandler);

    // try http connection
    payloadStr = "metric3.test 0 " + startTime + " source=test4\n" +
            "metric3.test 1 " + (startTime + 1) + " source=test5\n" +
            "metric3.test 2 " + (startTime + 2) + " source=test6"; // note the lack of newline at the end!
    assertEquals(202, httpPost("https://localhost:" + securePort, payloadStr));
    verify(mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerHttpGzipped() throws Exception {
    port = findAvailablePort(2888);
    int securePort = findAvailablePort(2889);
    proxy.proxyConfig.privateCertPath = getClass().getClassLoader().getResource("demo.cert").getPath();
    proxy.proxyConfig.privateKeyPath = getClass().getClassLoader().getResource("demo.key").getPath();
    proxy.proxyConfig.tlsPorts = "1,23 , 4,   , " + securePort +"  ,6";
    proxy.initSslContext();
    proxy.proxyConfig.pushListenerPorts = port + "," + securePort;
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D),
        proxy.proxyConfig.isTraceAlwaysSampleErrors());
    proxy.startGraphiteListener(String.valueOf(port), mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(String.valueOf(securePort), mockHandlerFactory, null, sampler);
    waitUntilListenerIsOnline(port);
    waitUntilListenerIsOnline(securePort);
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

    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric_4.test").setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric_4.test").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
            setMetric("metric_4.test").setHost("test3").setTimestamp((startTime + 2) * 1000).setValue(2.0d).build());
    expectLastCall();
    replay(mockPointHandler);

    // try secure http connection with gzip
    payloadStr = "metric_4.test 0 " + startTime + " source=test1\n" +
            "metric_4.test 1 " + (startTime + 1) + " source=test2\n" +
            "metric_4.test 2 " + (startTime + 2) + " source=test3"; // note the lack of newline at the end!
    gzippedHttpPost("https://localhost:" + securePort, payloadStr);
    verify(mockPointHandler);
  }

  // test that histograms received on Wavefront port get routed to the correct handler
  @Test
  public void testHistogramDataOnWavefrontUnifiedPortHandlerPlaintextUncompressed() throws Exception {
    port = findAvailablePort(2888);
    proxy.proxyConfig.pushListenerPorts = String.valueOf(port);
    proxy.startGraphiteListener(proxy.proxyConfig.getPushListenerPorts(), mockHandlerFactory,
        null, new SpanSampler(new RateSampler(1.0D),
            proxy.proxyConfig.isTraceAlwaysSampleErrors()));
    waitUntilListenerIsOnline(port);
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
    verifyWithTimeout(500, mockHistogramHandler);
  }

  // test Wavefront port handler with mixed payload: metrics, histograms, source tags
  @Test
  public void testWavefrontUnifiedPortHandlerPlaintextUncompressedMixedDataPayload() throws Exception {
    port = findAvailablePort(2888);
    proxy.proxyConfig.pushListenerPorts = String.valueOf(port);
    proxy.startGraphiteListener(proxy.proxyConfig.getPushListenerPorts(), mockHandlerFactory,
        null, new SpanSampler(new RateSampler(1.0D),
            proxy.proxyConfig.isTraceAlwaysSampleErrors()));
    waitUntilListenerIsOnline(port);
    reset(mockHistogramHandler);
    reset(mockPointHandler);
    reset(mockSourceTagHandler);
    reset(mockEventHandler);
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
    mockEventHandler.report(ReportEvent.newBuilder().
        setStartTime(startTime * 1000).
        setEndTime(startTime * 1000 + 1).
        setName("Event name for testing").
        setHosts(ImmutableList.of("host1", "host2")).
        setDimensions(ImmutableMap.of("multi", ImmutableList.of("bar", "baz"))).
        setAnnotations(ImmutableMap.of("severity", "INFO")).
        setTags(ImmutableList.of("tag1")).
        build());
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.mixed").setHost("test2").setTimestamp((startTime + 1) * 1000).
        setValue(9d).build());
    mockSourceTagHandler.report(ReportSourceTag.newBuilder().
        setOperation(SourceOperationType.SOURCE_TAG).setAction(SourceTagAction.SAVE).
        setSource("testSource").setAnnotations(ImmutableList.of("newtag1", "newtag2")).build());
    expectLastCall();
    replay(mockPointHandler);
    replay(mockHistogramHandler);
    replay(mockEventHandler);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr = "metric.test.mixed 10.0 " + (startTime + 1) + " source=test2\n" +
        "!M " + startTime + " #5 10.0 #10 100.0 metric.test.mixed source=test1\n" +
        "@SourceTag action=save source=testSource newtag1 newtag2\n" +
        "metric.test.mixed 9.0 " + (startTime + 1) + " source=test2\n" +
        "@Event " + startTime + " \"Event name for testing\" host=host1 host=host2 tag=tag1 " +
        "severity=INFO multi=bar multi=baz\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockPointHandler, mockHistogramHandler, mockEventHandler);
  }

  @Test
  public void testWavefrontHandlerAsDDIEndpoint() throws Exception {
    port = findAvailablePort(2978);
    proxy.proxyConfig.pushListenerPorts = String.valueOf(port);
    proxy.proxyConfig.dataBackfillCutoffHours = 8640;
    proxy.startGraphiteListener(proxy.proxyConfig.getPushListenerPorts(), mockHandlerFactory,
        null, new SpanSampler(new DurationSampler(5000),
            proxy.proxyConfig.isTraceAlwaysSampleErrors()));
    waitUntilListenerIsOnline(port);
    String traceId = UUID.randomUUID().toString();
    long timestamp1 = startTime * 1000000 + 12345;
    long timestamp2 = startTime * 1000000 + 23456;

    String payloadStr = "metric4.test 0 " + startTime + " source=test1\n" +
        "metric4.test 1 " + (startTime + 1) + " source=test2\n" +
        "metric4.test 2 " + (startTime + 2) + " source=test3"; // note the lack of newline at the end!
    String histoData = "!M " + startTime + " #5 10.0 #10 100.0 metric.test.histo source=test1\n" +
        "!M " + (startTime + 60) + " #5 20.0 #6 30.0 #7 40.0 metric.test.histo source=test2";
    String spanData = "testSpanName parent=parent1 source=testsource spanId=testspanid " +
        "traceId=\"" + traceId + "\" parent=parent2 " + startTime + " " + (startTime + 10);
    String spanDataToDiscard = "testSpanName parent=parent1 source=testsource spanId=testspanid " +
        "traceId=\"" + traceId + "\" parent=parent2 " + startTime + " " + (startTime + 1);
    String spanLogData = "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId +
        "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" +
        timestamp2 + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n";
    String spanLogDataWithSpanField = "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId +
        "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" +
        timestamp2 + ",\"fields\":{\"key3\":\"value3\"}}]," +
        "\"span\":\"" + escapeSpanData(spanData) + "\"}\n";
    String spanLogDataWithSpanFieldToDiscard =
        "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId +
        "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}}]," +
        "\"span\":\"" + escapeSpanData(spanDataToDiscard) + "\"}\n";
    String mixedData = "@SourceTag action=save source=testSource newtag1 newtag2\n" +
        "@Event " + startTime + " \"Event name for testing\" host=host1 host=host2 tag=tag1 " +
        "severity=INFO multi=bar multi=baz\n" +
        "!M " + (startTime + 60) + " #5 20.0 #6 30.0 #7 40.0 metric.test.histo source=test2\n" +
        "metric4.test 0 " + startTime + " source=test1\n" + spanLogData + spanLogDataWithSpanField;

    String invalidData = "{\"spanId\"}\n@SourceTag\n@Event\n!M #5\nmetric.name\n" +
        "metric5.test 0 1234567890 source=test1\n";

    reset(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test1").setTimestamp(startTime * 1000).
        setValue(0.0d).build());
    expectLastCall().times(2);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test2").setTimestamp((startTime + 1) * 1000).
        setValue(1.0d).build());
    expectLastCall().times(2);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test3").setTimestamp((startTime + 2) * 1000).
        setValue(2.0d).build());
    expectLastCall().times(2);
    replay(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);

    assertEquals(202, gzippedHttpPost("http://localhost:" + port + "/report", payloadStr));
    assertEquals(202, gzippedHttpPost("http://localhost:" + port +
        "/report?format=wavefront", payloadStr));
    verify(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);

    reset(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);
    mockHistogramHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.histo").setHost("test1").setTimestamp(startTime * 1000).setValue(
        Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setDuration(60000)
            .setBins(ImmutableList.of(10.0d, 100.0d))
            .setCounts(ImmutableList.of(5, 10))
            .build())
        .build());
    expectLastCall();
    mockHistogramHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.histo").setHost("test2").setTimestamp((startTime + 60) * 1000).
        setValue(Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setDuration(60000)
            .setBins(ImmutableList.of(20.0d, 30.0d, 40.0d))
            .setCounts(ImmutableList.of(5, 6, 7))
            .build())
        .build());
    expectLastCall();
    replay(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);

    assertEquals(202, gzippedHttpPost("http://localhost:" + port +
        "/report?format=histogram", histoData));
    verify(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);

    reset(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);
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
                setFields(ImmutableMap.of("key3", "value3")).
                build()
        )).
        build());
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime * 1000)
        .setDuration(10000)
        .setName("testSpanName")
        .setSource("testsource")
        .setSpanId("testspanid")
        .setTraceId(traceId)
        .setAnnotations(ImmutableList.of(new Annotation("parent", "parent1"),
            new Annotation("parent", "parent2")))
        .build());
    expectLastCall();
    replay(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);

    assertEquals(202, gzippedHttpPost("http://localhost:" + port +
        "/report?format=trace", spanData));
    assertEquals(202, gzippedHttpPost("http://localhost:" + port +
        "/report?format=spanLogs", spanLogData));
    assertEquals(202, gzippedHttpPost("http://localhost:" + port +
        "/report?format=spanLogs", spanLogDataWithSpanField));
    assertEquals(202, gzippedHttpPost("http://localhost:" + port +
        "/report?format=trace", spanDataToDiscard));
    assertEquals(202, gzippedHttpPost("http://localhost:" + port +
        "/report?format=spanLogs", spanLogDataWithSpanFieldToDiscard));
    verify(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);

    reset(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);
    mockSourceTagHandler.report(ReportSourceTag.newBuilder().
        setOperation(SourceOperationType.SOURCE_TAG).setAction(SourceTagAction.SAVE).
        setSource("testSource").setAnnotations(ImmutableList.of("newtag1", "newtag2")).build());
    expectLastCall();
    mockEventHandler.report(ReportEvent.newBuilder().setStartTime(startTime * 1000).
        setEndTime(startTime * 1000 + 1).setName("Event name for testing").
        setHosts(ImmutableList.of("host1", "host2")).setTags(ImmutableList.of("tag1")).
        setAnnotations(ImmutableMap.of("severity", "INFO")).
        setDimensions(ImmutableMap.of("multi", ImmutableList.of("bar", "baz"))).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test1").setTimestamp(startTime * 1000).
        setValue(0.0d).build());
    expectLastCall();
    replay(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);

    proxy.entityProps.get(ReportableEntityType.HISTOGRAM).setFeatureDisabled(true);
    assertEquals(403, gzippedHttpPost("http://localhost:" + port +
        "/report?format=histogram", histoData));
    proxy.entityProps.get(ReportableEntityType.TRACE).setFeatureDisabled(true);
    assertEquals(403, gzippedHttpPost("http://localhost:" + port +
        "/report?format=trace", spanData));
    proxy.entityProps.get(ReportableEntityType.TRACE_SPAN_LOGS).setFeatureDisabled(true);
    assertEquals(403, gzippedHttpPost("http://localhost:" + port +
        "/report?format=spanLogs", spanLogData));
    assertEquals(403, gzippedHttpPost("http://localhost:" + port +
        "/report?format=spanLogs", spanLogDataWithSpanField));
    assertEquals(202, gzippedHttpPost("http://localhost:" + port + "/report", mixedData));
    verify(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);

    reset(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);
    mockSourceTagHandler.report(ReportSourceTag.newBuilder().
        setOperation(SourceOperationType.SOURCE_TAG).setAction(SourceTagAction.SAVE).
        setSource("testSource").setAnnotations(ImmutableList.of("newtag1", "newtag2")).build());
    expectLastCall();
    mockEventHandler.report(ReportEvent.newBuilder().setStartTime(startTime * 1000).
        setEndTime(startTime * 1000 + 1).setName("Event name for testing").
        setHosts(ImmutableList.of("host1", "host2")).setTags(ImmutableList.of("tag1")).
        setAnnotations(ImmutableMap.of("severity", "INFO")).
        setDimensions(ImmutableMap.of("multi", ImmutableList.of("bar", "baz"))).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").setMetric("metric4.test").
        setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockSourceTagHandler.reject(eq("@SourceTag"), anyString());
    expectLastCall();
    mockEventHandler.reject(eq("@Event"), anyString());
    expectLastCall();
    mockPointHandler.reject(eq("metric.name"), anyString());
    expectLastCall();
    mockPointHandler.reject(eq(ReportPoint.newBuilder().setTable("dummy").setMetric("metric5.test").
        setHost("test1").setTimestamp(1234567890000L).setValue(0.0d).build()),
        startsWith("WF-402: Point outside of reasonable timeframe"));
    expectLastCall();
    replay(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);

    assertEquals(202, gzippedHttpPost("http://localhost:" + port + "/report",
        mixedData + "\n" + invalidData));

    verify(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler,
        mockSourceTagHandler, mockEventHandler);
  }

  @Test
  public void testTraceUnifiedPortHandlerPlaintextDebugSampling() throws Exception {
    tracePort = findAvailablePort(3888);
    proxy.proxyConfig.traceListenerPorts = String.valueOf(tracePort);
    proxy.startTraceListener(proxy.proxyConfig.getTraceListenerPorts(), mockHandlerFactory,
        new SpanSampler(new RateSampler(0.0D), false));
    waitUntilListenerIsOnline(tracePort);
    reset(mockTraceHandler);
    reset(mockTraceSpanLogsHandler);
    String traceId = UUID.randomUUID().toString();
    long timestamp1 = startTime * 1000000 + 12345;
    long timestamp2 = startTime * 1000000 + 23456;
    String spanData = "testSpanName parent=parent1 source=testsource spanId=testspanid " +
        "traceId=\"" + traceId + "\" debug=true " + startTime + " " + (startTime + 1) + "\n";
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
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime * 1000).
        setDuration(1000).
        setName("testSpanName").
        setSource("testsource").
        setSpanId("testspanid").
        setTraceId(traceId).
        setAnnotations(ImmutableList.of(
            new Annotation("parent", "parent1"),
            new Annotation("debug", "true"))).build());
    expectLastCall();
    replay(mockTraceHandler);
    replay(mockTraceSpanLogsHandler);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", tracePort);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr = spanData +
        "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId + "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" + timestamp2 +
        ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n" +
        "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId + "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" + timestamp2 +
        ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]," +
        "\"span\":\"" + escapeSpanData(spanData) + "\"}\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockTraceHandler, mockTraceSpanLogsHandler);
  }

  @Test
  public void testTraceUnifiedPortHandlerPlaintext() throws Exception {
    tracePort = findAvailablePort(3888);
    proxy.proxyConfig.traceListenerPorts = String.valueOf(tracePort);
    proxy.startTraceListener(proxy.proxyConfig.getTraceListenerPorts(), mockHandlerFactory,
        new SpanSampler(new RateSampler(1.0D), proxy.proxyConfig.isTraceAlwaysSampleErrors()));
    waitUntilListenerIsOnline(tracePort);
    reset(mockTraceHandler);
    reset(mockTraceSpanLogsHandler);
    String traceId = UUID.randomUUID().toString();
    long timestamp1 = startTime * 1000000 + 12345;
    long timestamp2 = startTime * 1000000 + 23456;
    String spanData = "testSpanName parent=parent1 source=testsource spanId=testspanid " +
        "traceId=\"" + traceId + "\" parent=parent2 " + startTime + " " + (startTime + 1) + "\n";
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
    String payloadStr = spanData +
        "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId + "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" + timestamp2 +
        ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n" +
        "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId + "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" + timestamp2 +
        ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]," +
        "\"span\":\"" + escapeSpanData(spanData) + "\"}\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockTraceHandler, mockTraceSpanLogsHandler);
  }

  @Test
  public void testCustomTraceUnifiedPortHandlerDerivedMetrics() throws Exception {
    customTracePort = findAvailablePort(51233);
    proxy.proxyConfig.customTracingListenerPorts = String.valueOf(customTracePort);
    setUserPreprocessorForTraceDerivedREDMetrics(customTracePort);
    proxy.startCustomTracingListener(proxy.proxyConfig.getCustomTracingListenerPorts(),
        mockHandlerFactory, mockWavefrontSender, new SpanSampler(new RateSampler(1.0D),
            proxy.proxyConfig.isTraceAlwaysSampleErrors()));
    waitUntilListenerIsOnline(customTracePort);
    reset(mockTraceHandler);
    reset(mockWavefrontSender);

    String traceId = UUID.randomUUID().toString();
    String spanData = "testSpanName source=testsource spanId=testspanid " +
        "traceId=\"" + traceId + "\" " + startTime + " " + (startTime + 1) + "\n";

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime * 1000).
        setDuration(1000).
        setName("testSpanName").
        setSource(PREPROCESSED_SOURCE_VALUE).
        setSpanId("testspanid").
        setTraceId(traceId).
        setAnnotations(ImmutableList.of(
            new Annotation("application", PREPROCESSED_APPLICATION_TAG_VALUE),
            new Annotation("service", PREPROCESSED_SERVICE_TAG_VALUE),
            new Annotation("cluster", PREPROCESSED_CLUSTER_TAG_VALUE),
            new Annotation("shard", PREPROCESSED_SHARD_TAG_VALUE))).build());
    expectLastCall();

    Capture<HashMap<String, String>> tagsCapture = EasyMock.newCapture();
    mockWavefrontSender.sendMetric(eq(HEART_BEAT_METRIC), eq(1.0), anyLong(),
        eq(PREPROCESSED_SOURCE_VALUE), EasyMock.capture(tagsCapture));
    expectLastCall().anyTimes();
    replay(mockTraceHandler, mockWavefrontSender);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", customTracePort);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    stream.write(spanData.getBytes());
    stream.flush();
    socket.close();
    // sleep to get around "Nothing captured yet" issue with heartbeat metric call.
    Thread.sleep(100);
    verifyWithTimeout(500, mockTraceHandler, mockWavefrontSender);
    HashMap<String, String> tagsReturned = tagsCapture.getValue();
    assertEquals(PREPROCESSED_APPLICATION_TAG_VALUE, tagsReturned.get(APPLICATION_TAG_KEY));
    assertEquals(PREPROCESSED_SERVICE_TAG_VALUE, tagsReturned.get(SERVICE_TAG_KEY));
    assertEquals(PREPROCESSED_CLUSTER_TAG_VALUE, tagsReturned.get(CLUSTER_TAG_KEY));
    assertEquals(PREPROCESSED_SHARD_TAG_VALUE, tagsReturned.get(SHARD_TAG_KEY));
  }

  private void setUserPreprocessorForTraceDerivedREDMetrics(int port) {
    ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
    PreprocessorRuleMetrics preprocessorRuleMetrics = new PreprocessorRuleMetrics(null, null,
        null);
    preprocessor.forSpan().addTransformer(new SpanAddAnnotationIfNotExistsTransformer
        ("application", PREPROCESSED_APPLICATION_TAG_VALUE, x -> true, preprocessorRuleMetrics));
    preprocessor.forSpan().addTransformer(new SpanAddAnnotationIfNotExistsTransformer
        ("service", PREPROCESSED_SERVICE_TAG_VALUE, x -> true, preprocessorRuleMetrics));
    preprocessor.forSpan().addTransformer(new SpanAddAnnotationIfNotExistsTransformer
        ("cluster", PREPROCESSED_CLUSTER_TAG_VALUE, x -> true, preprocessorRuleMetrics));
    preprocessor.forSpan().addTransformer(new SpanAddAnnotationIfNotExistsTransformer
        ("shard", PREPROCESSED_SHARD_TAG_VALUE, x -> true, preprocessorRuleMetrics));
    preprocessor.forSpan().addTransformer(new SpanReplaceRegexTransformer("sourceName",
        "^test.*", PREPROCESSED_SOURCE_VALUE, null, null, false, x -> true, preprocessorRuleMetrics));
    Map<String, ReportableEntityPreprocessor> userPreprocessorMap = new HashMap<>();
    userPreprocessorMap.put(String.valueOf(port), preprocessor);
    proxy.preprocessors.userPreprocessors = userPreprocessorMap;
  }

  @Test
  public void testCustomTraceUnifiedPortHandlerPlaintext() throws Exception {
    customTracePort = findAvailablePort(50000);
    proxy.proxyConfig.customTracingListenerPorts = String.valueOf(customTracePort);
    proxy.startCustomTracingListener(proxy.proxyConfig.getCustomTracingListenerPorts(),
        mockHandlerFactory, mockWavefrontSender, new SpanSampler(new RateSampler(1.0D),
            proxy.proxyConfig.isTraceAlwaysSampleErrors()));
    waitUntilListenerIsOnline(customTracePort);
    reset(mockTraceHandler);
    reset(mockTraceSpanLogsHandler);
    reset(mockWavefrontSender);
    String traceId = UUID.randomUUID().toString();
    long timestamp1 = startTime * 1000000 + 12345;
    long timestamp2 = startTime * 1000000 + 23456;
    String spanData = "testSpanName source=testsource spanId=testspanid " +
        "traceId=\"" + traceId + "\" application=application1 service=service1 " + startTime +
        " " + (startTime + 1) + "\n";
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
        .setAnnotations(ImmutableList.of(new Annotation("application", "application1"),
            new Annotation("service", "service1"))).build());
    expectLastCall();
    Capture<HashMap<String, String>> tagsCapture = EasyMock.newCapture();
    mockWavefrontSender.sendMetric(eq(HEART_BEAT_METRIC), eq(1.0), anyLong(),
        eq("testsource"), EasyMock.capture(tagsCapture));
    EasyMock.expectLastCall().anyTimes();
    replay(mockTraceHandler, mockTraceSpanLogsHandler, mockWavefrontSender);
    Socket socket = SocketFactory.getDefault().createSocket("localhost", customTracePort);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr = spanData +
        "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId + "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" + timestamp2 +
        ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n" +
        "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId + "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" + timestamp2 +
        ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]," +
        "\"span\":\"" + escapeSpanData(spanData) + "\"}\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockTraceHandler, mockTraceSpanLogsHandler, mockWavefrontSender);
    HashMap<String, String> tagsReturned = tagsCapture.getValue();
    assertEquals("application1", tagsReturned.get(APPLICATION_TAG_KEY));
    assertEquals("service1", tagsReturned.get(SERVICE_TAG_KEY));
    assertEquals("none", tagsReturned.get(CLUSTER_TAG_KEY));
    assertEquals("none", tagsReturned.get(SHARD_TAG_KEY));
  }

  @Test(timeout = 30000)
  public void testDataDogUnifiedPortHandler() throws Exception {
    ddPort = findAvailablePort(4888);
    proxy.proxyConfig.dataDogJsonPorts = String.valueOf(ddPort);
    proxy.startDataDogListener(proxy.proxyConfig.getDataDogJsonPorts(), mockHandlerFactory,
        mockHttpClient);
    int ddPort2 = findAvailablePort(4988);
    PushAgent proxy2 = new PushAgent();
    proxy2.proxyConfig.flushThreads = 2;
    proxy2.proxyConfig.dataBackfillCutoffHours = 100000000;
    proxy2.proxyConfig.dataDogJsonPorts = String.valueOf(ddPort2);
    proxy2.proxyConfig.dataDogRequestRelaySyncMode = true;
    proxy2.proxyConfig.dataDogProcessSystemMetrics = true;
    proxy2.proxyConfig.dataDogProcessServiceChecks = false;
    proxy2.proxyConfig.dataDogRequestRelayTarget = "http://relay-to:1234";
    assertEquals(Integer.valueOf(2), proxy2.proxyConfig.getFlushThreads());
    assertTrue(proxy2.proxyConfig.isDataDogProcessSystemMetrics());
    assertFalse(proxy2.proxyConfig.isDataDogProcessServiceChecks());

    proxy2.startDataDogListener(proxy2.proxyConfig.getDataDogJsonPorts(), mockHandlerFactory,
        mockHttpClient);
    waitUntilListenerIsOnline(ddPort2);

    int ddPort3 = findAvailablePort(4990);
    PushAgent proxy3 = new PushAgent();
    proxy3.proxyConfig.dataBackfillCutoffHours = 100000000;
    proxy3.proxyConfig.dataDogJsonPorts = String.valueOf(ddPort3);
    proxy3.proxyConfig.dataDogProcessSystemMetrics = true;
    proxy3.proxyConfig.dataDogProcessServiceChecks = true;
    assertTrue(proxy3.proxyConfig.isDataDogProcessSystemMetrics());
    assertTrue(proxy3.proxyConfig.isDataDogProcessServiceChecks());

    proxy3.startDataDogListener(proxy3.proxyConfig.getDataDogJsonPorts(), mockHandlerFactory,
            mockHttpClient);
    waitUntilListenerIsOnline(ddPort3);

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

    // test 6: post to /api/v1/series including a /api/v1/intake call to ensure system host-tags are propogated
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().
        setTable("dummy").
        setMetric("system.net.tcp.retrans_segs").
        setHost("testhost").
        setTimestamp(1531176936000L).
        setValue(0.0d).
        setAnnotations(ImmutableMap.of("app", "closedstack", "role", "control")).
        build());
    expectLastCall().once();
    mockPointHandler.report(ReportPoint.newBuilder().
        setTable("dummy").
        setMetric("system.net.tcp.listen_drops").
        setHost("testhost").
        setTimestamp(1531176936000L).
        setValue(0.0d).
        setAnnotations(ImmutableMap.of("_source", "Launcher", "env", "prod",
                "app", "openstack", "role", "control")).
        build());
    expectLastCall().once();
    mockPointHandler.report(ReportPoint.newBuilder().
        setTable("dummy").
        setMetric("system.net.packets_in.count").
        setHost("testhost").
        setTimestamp(1531176936000L).
        setValue(12.052631578947368d).
        setAnnotations(ImmutableMap.of("device", "eth0", "app", "closedstack", "role", "control")).
        build());
    expectLastCall().once();
    mockPointHandler.report(ReportPoint.newBuilder().
            setTable("dummy").
            setMetric("test.metric").
            setHost("testhost").
            setTimestamp(1531176936000L).
            setValue(400.0d).
            setAnnotations(ImmutableMap.of("app", "closedstack", "role", "control")).
            build());
    expectLastCall().once();
    replay(mockPointHandler);
    gzippedHttpPost("http://localhost:" + ddPort3 + "/intake", getResource("ddTestSystemMetadataOnly.json"));
    gzippedHttpPost("http://localhost:" + ddPort3 + "/api/v1/series", getResource("ddTestTimeseries.json"));
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
    deltaPort = findAvailablePort(5888);
    proxy.proxyConfig.deltaCountersAggregationListenerPorts = String.valueOf(deltaPort);
    proxy.proxyConfig.deltaCountersAggregationIntervalSeconds = 10;
    proxy.proxyConfig.pushFlushInterval = 100;
    proxy.startDeltaCounterListener(proxy.proxyConfig.getDeltaCountersAggregationListenerPorts(),
        null, mockSenderTaskFactory, new SpanSampler(new RateSampler(1.0D),
            proxy.proxyConfig.isTraceAlwaysSampleErrors()));
    waitUntilListenerIsOnline(deltaPort);
    reset(mockSenderTask);
    Capture<String> capturedArgument = Capture.newInstance(CaptureType.ALL);
    mockSenderTask.add(EasyMock.capture(capturedArgument));
    expectLastCall().atLeastOnce();
    replay(mockSenderTask);

    String payloadStr1 = "∆test.mixed1 1.0 source=test1\n";
    String payloadStr2 = "∆test.mixed2 2.0 source=test1\n";
    String payloadStr3 = "test.mixed3 3.0 source=test1\n";
    String payloadStr4 = "∆test.mixed3 3.0 source=test1\n";
    assertEquals(202, httpPost("http://localhost:" + deltaPort, payloadStr1 + payloadStr2 +
        payloadStr2 + payloadStr3 + payloadStr4));
    ReportableEntityHandler<?, ?> handler = proxy.deltaCounterHandlerFactory.
        getHandler(HandlerKey.of(ReportableEntityType.POINT, String.valueOf(deltaPort)));
    if (handler instanceof DeltaCounterAccumulationHandlerImpl) {
      ((DeltaCounterAccumulationHandlerImpl) handler).flushDeltaCounters();
    }
    verify(mockSenderTask);
    assertEquals(3, capturedArgument.getValues().size());
    assertTrue(capturedArgument.getValues().get(0).startsWith("\"∆test.mixed1\" 1.0" ));
    assertTrue(capturedArgument.getValues().get(1).startsWith("\"∆test.mixed2\" 4.0" ));
    assertTrue(capturedArgument.getValues().get(2).startsWith("\"∆test.mixed3\" 3.0" ));
  }

  @Test
  public void testDeltaCounterHandlerDataStream() throws Exception {
    deltaPort = findAvailablePort(5888);
    proxy.proxyConfig.deltaCountersAggregationListenerPorts = String.valueOf(deltaPort);
    proxy.proxyConfig.deltaCountersAggregationIntervalSeconds = 10;
    proxy.startDeltaCounterListener(proxy.proxyConfig.getDeltaCountersAggregationListenerPorts(),
        null, mockSenderTaskFactory, new SpanSampler(new RateSampler(1.0D),
            proxy.proxyConfig.isTraceAlwaysSampleErrors()));
    waitUntilListenerIsOnline(deltaPort);
    reset(mockSenderTask);
    Capture<String> capturedArgument = Capture.newInstance(CaptureType.ALL);
    mockSenderTask.add(EasyMock.capture(capturedArgument));
    expectLastCall().atLeastOnce();
    replay(mockSenderTask);

    String payloadStr = "∆test.mixed 1.0 " + startTime + " source=test1\n";
    assertEquals(202, httpPost("http://localhost:" + deltaPort, payloadStr + payloadStr));
    ReportableEntityHandler<?, ?> handler = proxy.deltaCounterHandlerFactory.
        getHandler(HandlerKey.of(ReportableEntityType.POINT, String.valueOf(deltaPort)));
    if (!(handler instanceof DeltaCounterAccumulationHandlerImpl)) fail();
    ((DeltaCounterAccumulationHandlerImpl) handler).flushDeltaCounters();

    assertEquals(202, httpPost("http://localhost:" + deltaPort, payloadStr));
    assertEquals(202, httpPost("http://localhost:" + deltaPort, payloadStr + payloadStr));
    ((DeltaCounterAccumulationHandlerImpl) handler).flushDeltaCounters();
    verify(mockSenderTask);
    assertEquals(2, capturedArgument.getValues().size());
    assertTrue(capturedArgument.getValues().get(0).startsWith("\"∆test.mixed\" 2.0" ));
    assertTrue(capturedArgument.getValues().get(1).startsWith("\"∆test.mixed\" 3.0" ));
  }

  @Test
  public void testOpenTSDBPortHandler() throws Exception {
    port = findAvailablePort(4242);
    proxy.proxyConfig.opentsdbPorts = String.valueOf(port);
    proxy.startOpenTsdbListener(proxy.proxyConfig.getOpentsdbPorts(), mockHandlerFactory);
    waitUntilListenerIsOnline(port);
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test1").setTimestamp(startTime * 1000).
        setAnnotations(ImmutableMap.of("env", "prod")).setValue(0.0d).build());
    expectLastCall().times(2);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test2").setTimestamp((startTime + 1) * 1000).
        setAnnotations(ImmutableMap.of("env", "prod")).setValue(1.0d).build());
    expectLastCall().times(2);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test3").setTimestamp((startTime + 2) * 1000).
        setAnnotations(ImmutableMap.of("env", "prod")).setValue(2.0d).build());
    expectLastCall().times(2);
    mockPointHandler.reject((ReportPoint) eq(null), anyString());
    expectLastCall().once();
    replay(mockPointHandler);

    String payloadStr = "[\n" +
        "  {\n" +
        "    \"metric\": \"metric4.test\",\n" +
        "    \"timestamp\": " + startTime + ",\n" +
        "    \"value\": 0.0,\n" +
        "    \"tags\": {\n" +
        "      \"host\": \"test1\",\n" +
        "      \"env\": \"prod\"\n" +
        "    }\n" +
        "  },\n" +
        "  {\n" +
        "    \"metric\": \"metric4.test\",\n" +
        "    \"timestamp\": " + (startTime + 1) + ",\n" +
        "    \"value\": 1.0,\n" +
        "    \"tags\": {\n" +
        "      \"host\": \"test2\",\n" +
        "      \"env\": \"prod\"\n" +
        "    }\n" +
        "  }\n" +
        "]\n";
    String payloadStr2 = "[\n" +
        "  {\n" +
        "    \"metric\": \"metric4.test\",\n" +
        "    \"timestamp\": " + (startTime + 2) + ",\n" +
        "    \"value\": 2.0,\n" +
        "    \"tags\": {\n" +
        "      \"host\": \"test3\",\n" +
        "      \"env\": \"prod\"\n" +
        "    }\n" +
        "  },\n" +
        "  {\n" +
        "    \"metric\": \"metric4.test\",\n" +
        "    \"timestamp\": " + startTime + ",\n" +
        "    \"tags\": {\n" +
        "      \"host\": \"test4\",\n" +
        "      \"env\": \"prod\"\n" +
        "    }\n" +
        "  }]\n";

    Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String points = "version\n" +
        "put metric4.test " + startTime + " 0 host=test1 env=prod\n" +
        "put metric4.test " + (startTime + 1) + " 1 host=test2 env=prod\n" +
        "put metric4.test " + (startTime + 2) + " 2 host=test3 env=prod\n";
    stream.write(points.getBytes());
    stream.flush();
    socket.close();

    // nonexistent path should return 400
    assertEquals(400, gzippedHttpPost("http://localhost:" + port + "/api/nonexistent", ""));
    assertEquals(200, gzippedHttpPost("http://localhost:" + port + "/api/version", ""));
    // malformed json should return 400
    assertEquals(400, gzippedHttpPost("http://localhost:" + port + "/api/put", "{]"));
    assertEquals(204, gzippedHttpPost("http://localhost:" + port + "/api/put", payloadStr));
    // 1 good, 1 invalid point - should return 400, but good point should still go through
    assertEquals(400, gzippedHttpPost("http://localhost:" + port + "/api/put", payloadStr2));

    verify(mockPointHandler);
  }

  @Test
  public void testJsonMetricsPortHandler() throws Exception {
    port = findAvailablePort(3878);
    proxy.proxyConfig.jsonListenerPorts = String.valueOf(port);
    proxy.startJsonListener(proxy.proxyConfig.jsonListenerPorts, mockHandlerFactory);
    waitUntilListenerIsOnline(port);
    reset(mockPointHandler);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test").setHost("testSource").setTimestamp(startTime * 1000).
        setAnnotations(ImmutableMap.of("env", "prod", "dc", "test1")).setValue(1.0d).build());
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.cpu.usage.idle").setHost("testSource").
        setTimestamp(startTime * 1000).setValue(99.0d).build());
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.cpu.usage.user").setHost("testSource").
        setTimestamp(startTime * 1000).setValue(0.5d).build());
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.cpu.usage.system").setHost("testSource").
        setTimestamp(startTime * 1000).setValue(0.7d).build());
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.disk.free").setHost("testSource").
        setTimestamp(startTime * 1000).setValue(0.0d).build());
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.mem.used").setHost("testSource").
        setTimestamp(startTime * 1000).setValue(50.0d).build());
    replay(mockPointHandler);

    String payloadStr = "{\n" +
        "  \"value\": 1.0,\n" +
        "  \"tags\": {\n" +
        "    \"dc\": \"test1\",\n" +
        "    \"env\": \"prod\"\n" +
        "  }\n" +
        "}\n";
    String payloadStr2 = "{\n" +
        "  \"cpu.usage\": {\n" +
        "    \"idle\": 99.0,\n" +
       "    \"user\": 0.5,\n" +
        "    \"system\": 0.7\n" +
        "  },\n" +
        "  \"disk.free\": 0.0,\n" +
        "  \"mem\": {\n" +
        "    \"used\": 50.0\n" +
        "  }\n" +
        "}\n";
    assertEquals(200, gzippedHttpPost("http://localhost:" + port + "/?h=testSource&p=metric.test&" +
        "d=" + startTime * 1000, payloadStr));
    assertEquals(200, gzippedHttpPost("http://localhost:" + port + "/?h=testSource&p=metric.test&" +
        "d=" + startTime * 1000, payloadStr2));
    verify(mockPointHandler);
  }

  @Test
  public void testWriteHttpJsonMetricsPortHandler() throws Exception {
    port = findAvailablePort(4878);
    proxy.proxyConfig.writeHttpJsonListenerPorts = String.valueOf(port);
    proxy.proxyConfig.hostname = "defaultLocalHost";
    proxy.startWriteHttpJsonListener(proxy.proxyConfig.writeHttpJsonListenerPorts,
        mockHandlerFactory);
    waitUntilListenerIsOnline(port);
    reset(mockPointHandler);
    mockPointHandler.reject((ReportPoint) eq(null), anyString());
    expectLastCall().times(2);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("disk.sda.disk_octets.read").setHost("testSource").
        setTimestamp(startTime * 1000).setValue(197141504).build());
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("disk.sda.disk_octets.write").setHost("testSource").
        setTimestamp(startTime * 1000).setValue(175136768).build());
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("disk.sda.disk_octets.read").setHost("defaultLocalHost").
        setTimestamp(startTime * 1000).setValue(297141504).build());
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("disk.sda.disk_octets.write").setHost("defaultLocalHost").
        setTimestamp(startTime * 1000).setValue(275136768).build());
    replay(mockPointHandler);
    String payloadStr = "[{\n" +
        "\"values\": [197141504, 175136768],\n" +
        "\"dstypes\": [\"counter\", \"counter\"],\n" +
        "\"dsnames\": [\"read\", \"write\"],\n" +
        "\"time\": " + startTime + ",\n" +
        "\"interval\": 10,\n" +
        "\"host\": \"testSource\",\n" +
        "\"plugin\": \"disk\",\n" +
        "\"plugin_instance\": \"sda\",\n" +
        "\"type\": \"disk_octets\",\n" +
        "\"type_instance\": \"\"\n" +
        "},{\n" +
        "\"values\": [297141504, 275136768],\n" +
        "\"dstypes\": [\"counter\", \"counter\"],\n" +
        "\"dsnames\": [\"read\", \"write\"],\n" +
        "\"time\": " + startTime + ",\n" +
        "\"interval\": 10,\n" +
        "\"plugin\": \"disk\",\n" +
        "\"plugin_instance\": \"sda\",\n" +
        "\"type\": \"disk_octets\",\n" +
        "\"type_instance\": \"\"\n" +
        "},{\n" +
        "\"dstypes\": [\"counter\", \"counter\"],\n" +
        "\"dsnames\": [\"read\", \"write\"],\n" +
        "\"time\": " + startTime + ",\n" +
        "\"interval\": 10,\n" +
        "\"plugin\": \"disk\",\n" +
        "\"plugin_instance\": \"sda\",\n" +
        "\"type\": \"disk_octets\",\n" +
        "\"type_instance\": \"\"\n" +
        "}]\n";

        // should be an array
    assertEquals(400, gzippedHttpPost("http://localhost:" + port, "{}"));
    assertEquals(200, gzippedHttpPost("http://localhost:" + port, payloadStr));
    verify(mockPointHandler);
  }

  @Test
  public void testRelayPortHandlerGzipped() throws Exception {
    port = findAvailablePort(2888);
    proxy.proxyConfig.pushRelayListenerPorts = String.valueOf(port);
    proxy.proxyConfig.pushRelayHistogramAggregator = true;
    proxy.proxyConfig.pushRelayHistogramAggregatorAccumulatorSize = 10L;
    proxy.proxyConfig.pushRelayHistogramAggregatorFlushSecs = 1;
    proxy.startRelayListener(proxy.proxyConfig.getPushRelayListenerPorts(), mockHandlerFactory,
        null);
    waitUntilListenerIsOnline(port);
    reset(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler);
    String traceId = UUID.randomUUID().toString();
    long timestamp1 = startTime * 1000000 + 12345;
    long timestamp2 = startTime * 1000000 + 23456;
    String spanData = "testSpanName parent=parent1 source=testsource spanId=testspanid " +
        "traceId=\"" + traceId + "\" parent=parent2 " + startTime + " " + (startTime + 1);
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test1").setTimestamp(startTime * 1000).setValue(0.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test2").setTimestamp((startTime + 1) * 1000).setValue(1.0d).build());
    expectLastCall();
    mockPointHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric4.test").setHost("test3").setTimestamp((startTime + 2) * 1000).setValue(2.0d).build());
    expectLastCall();
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
                setFields(ImmutableMap.of("key3", "value3")).
                build()
        )).
        setSpan(spanData).
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
    mockPointHandler.reject(anyString(), anyString());
    expectLastCall().times(2);

    replay(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler);

    String payloadStr = "metric4.test 0 " + startTime + " source=test1\n" +
        "metric4.test 1 " + (startTime + 1) + " source=test2\n" +
        "metric4.test 2 " + (startTime + 2) + " source=test3"; // note the lack of newline at the end!
    String histoData = "!M " + startTime + " #5 10.0 #10 100.0 metric.test.histo source=test1\n" +
        "!M " + (startTime + 60) + " #5 20.0 #6 30.0 #7 40.0 metric.test.histo source=test2";
    String spanLogData = "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId +
        "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" +
        timestamp2 + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n";
    String spanLogDataWithSpanField = "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId +
        "\",\"logs\":[{\"timestamp\":" + timestamp1 +
        ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" +
        timestamp2 + ",\"fields\":{\"key3\":\"value3\"}}]," +
        "\"span\":\"" + escapeSpanData(spanData) + "\"}\n";
    String badData = "@SourceTag action=save source=testSource newtag1 newtag2\n" +
        "@Event " + startTime + " \"Event name for testing\" host=host1 host=host2 tag=tag1 " +
        "severity=INFO multi=bar multi=baz\n" +
        "!M " + (startTime + 60) + " #5 20.0 #6 30.0 #7 40.0 metric.test.histo source=test2";

    assertEquals(200, gzippedHttpPost("http://localhost:" + port + "/api/v2/wfproxy/checkin",
        "{}"));
    assertEquals(200, gzippedHttpPost("http://localhost:" + port +
        "/api/v2/wfproxy/report?format=wavefront", payloadStr));
    assertEquals(200, gzippedHttpPost("http://localhost:" + port +
        "/api/v2/wfproxy/report?format=histogram", histoData));
    assertEquals(200, gzippedHttpPost("http://localhost:" + port +
        "/api/v2/wfproxy/report?format=trace", spanData));
    assertEquals(200, gzippedHttpPost("http://localhost:" + port +
        "/api/v2/wfproxy/report?format=spanLogs", spanLogData));
    assertEquals(200, gzippedHttpPost("http://localhost:" + port +
        "/api/v2/wfproxy/report?format=spanLogs", spanLogDataWithSpanField));
    proxy.entityProps.get(ReportableEntityType.HISTOGRAM).setFeatureDisabled(true);
    assertEquals(403, gzippedHttpPost("http://localhost:" + port +
        "/api/v2/wfproxy/report?format=histogram", histoData));
    proxy.entityProps.get(ReportableEntityType.TRACE).setFeatureDisabled(true);
    assertEquals(403, gzippedHttpPost("http://localhost:" + port +
        "/api/v2/wfproxy/report?format=trace", spanData));
    proxy.entityProps.get(ReportableEntityType.TRACE_SPAN_LOGS).setFeatureDisabled(true);
    assertEquals(403, gzippedHttpPost("http://localhost:" + port +
        "/api/v2/wfproxy/report?format=spanLogs", spanLogData));
    assertEquals(403, gzippedHttpPost("http://localhost:" + port +
        "/api/v2/wfproxy/report?format=spanLogs", spanLogDataWithSpanField));
    assertEquals(400, gzippedHttpPost("http://localhost:" + port +
        "/api/v2/wfproxy/report?format=wavefront", badData));
    verify(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler);
  }

  @Test
  public void testHealthCheckAdminPorts() throws Exception {
    port = findAvailablePort(2888);
    int port2 = findAvailablePort(3888);
    int port3 = findAvailablePort(4888);
    int port4 = findAvailablePort(5888);
    int adminPort = findAvailablePort(6888);
    proxy.proxyConfig.pushListenerPorts = port + "," + port2 + "," + port3 + "," + port4;
    proxy.proxyConfig.adminApiListenerPort = adminPort;
    proxy.proxyConfig.httpHealthCheckPath = "/health";
    proxy.proxyConfig.httpHealthCheckAllPorts = true;
    proxy.proxyConfig.httpHealthCheckFailStatusCode = 403;
    proxy.healthCheckManager = new HealthCheckManagerImpl(proxy.proxyConfig);
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D),
        proxy.proxyConfig.isTraceAlwaysSampleErrors());
    proxy.startGraphiteListener(String.valueOf(port), mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(String.valueOf(port2), mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(String.valueOf(port3), mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(String.valueOf(port4), mockHandlerFactory, null, sampler);
    proxy.startAdminListener(adminPort);
    waitUntilListenerIsOnline(adminPort);
    assertEquals(404, httpGet("http://localhost:" + adminPort + "/"));
    assertEquals(200, httpGet("http://localhost:" + port + "/health"));
    assertEquals(200, httpGet("http://localhost:" + port2 + "/health"));
    assertEquals(200, httpGet("http://localhost:" + port3 + "/health"));
    assertEquals(200, httpGet("http://localhost:" + port4 + "/health"));
    assertEquals(202, httpGet("http://localhost:" + port + "/health2"));
    assertEquals(400, httpGet("http://localhost:" + adminPort + "/status"));
    assertEquals(405, httpPost("http://localhost:" + adminPort + "/status", ""));
    assertEquals(404, httpGet("http://localhost:" + adminPort + "/status/somethingelse"));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port2));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port3));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port4));
    assertEquals(405, httpGet("http://localhost:" + adminPort + "/disable"));
    assertEquals(405, httpGet("http://localhost:" + adminPort + "/enable"));
    assertEquals(405, httpGet("http://localhost:" + adminPort + "/disable/" + port));
    assertEquals(405, httpGet("http://localhost:" + adminPort + "/enable/" + port));

    // disabling port and port3
    assertEquals(200, httpPost("http://localhost:" + adminPort + "/disable/" + port, ""));
    assertEquals(200, httpPost("http://localhost:" + adminPort + "/disable/" + port3, ""));
    assertEquals(503, httpGet("http://localhost:" + adminPort + "/status/" + port));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port2));
    assertEquals(503, httpGet("http://localhost:" + adminPort + "/status/" + port3));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port4));
    assertEquals(403, httpGet("http://localhost:" + port + "/health"));
    assertEquals(200, httpGet("http://localhost:" + port2 + "/health"));
    assertEquals(403, httpGet("http://localhost:" + port3 + "/health"));
    assertEquals(200, httpGet("http://localhost:" + port4 + "/health"));

    // disable all
    assertEquals(200, httpPost("http://localhost:" + adminPort + "/disable", ""));
    assertEquals(503, httpGet("http://localhost:" + adminPort + "/status/" + port));
    assertEquals(503, httpGet("http://localhost:" + adminPort + "/status/" + port2));
    assertEquals(503, httpGet("http://localhost:" + adminPort + "/status/" + port3));
    assertEquals(503, httpGet("http://localhost:" + adminPort + "/status/" + port4));
    assertEquals(403, httpGet("http://localhost:" + port + "/health"));
    assertEquals(403, httpGet("http://localhost:" + port2 + "/health"));
    assertEquals(403, httpGet("http://localhost:" + port3 + "/health"));
    assertEquals(403, httpGet("http://localhost:" + port4 + "/health"));

    // enable port3 and port4
    assertEquals(200, httpPost("http://localhost:" + adminPort + "/enable/" + port3, ""));
    assertEquals(200, httpPost("http://localhost:" + adminPort + "/enable/" + port4, ""));
    assertEquals(503, httpGet("http://localhost:" + adminPort + "/status/" + port));
    assertEquals(503, httpGet("http://localhost:" + adminPort + "/status/" + port2));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port3));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port4));
    assertEquals(403, httpGet("http://localhost:" + port + "/health"));
    assertEquals(403, httpGet("http://localhost:" + port2 + "/health"));
    assertEquals(200, httpGet("http://localhost:" + port3 + "/health"));
    assertEquals(200, httpGet("http://localhost:" + port4 + "/health"));

    // enable all
    // enable port3 and port4
    assertEquals(200, httpPost("http://localhost:" + adminPort + "/enable", ""));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port2));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port3));
    assertEquals(200, httpGet("http://localhost:" + adminPort + "/status/" + port4));
    assertEquals(200, httpGet("http://localhost:" + port + "/health"));
    assertEquals(200, httpGet("http://localhost:" + port2 + "/health"));
    assertEquals(200, httpGet("http://localhost:" + port3 + "/health"));
    assertEquals(200, httpGet("http://localhost:" + port4 + "/health"));
  }

  @Test
  public void testLargeHistogramDataOnWavefrontUnifiedPortHandler() throws Exception {
    port = findAvailablePort(2988);
    proxy.proxyConfig.pushListenerPorts = String.valueOf(port);
    proxy.startGraphiteListener(proxy.proxyConfig.getPushListenerPorts(), mockHandlerFactory,
        null, new SpanSampler(new RateSampler(1.0D),
            proxy.proxyConfig.isTraceAlwaysSampleErrors()));
    waitUntilListenerIsOnline(port);
    reset(mockHistogramHandler);
    List<Double> bins = new ArrayList<>();
    List<Integer> counts = new ArrayList<>();
    for (int i = 0; i < 50; i++) bins.add(10.0d);
    for (int i = 0; i < 150; i++) bins.add(99.0d);
    for (int i = 0; i < 200; i++) counts.add(1);
    mockHistogramHandler.report(ReportPoint.newBuilder().setTable("dummy").
        setMetric("metric.test.histo").setHost("test1").setTimestamp(startTime * 1000).setValue(
        Histogram.newBuilder()
            .setType(HistogramType.TDIGEST)
            .setDuration(60000)
            .setBins(bins)
            .setCounts(counts)
            .build())
        .build());
    expectLastCall();
    replay(mockHistogramHandler);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    StringBuilder payloadStr = new StringBuilder("!M ");
    payloadStr.append(startTime);
    for (int i = 0; i < 50; i++) payloadStr.append(" #1 10.0");
    for (int i = 0; i < 150; i++) payloadStr.append(" #1 99.0");
    payloadStr.append(" metric.test.histo source=test1\n");
    stream.write(payloadStr.toString().getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockHistogramHandler);
  }

  private String escapeSpanData(String spanData) {
    return spanData.replace("\"", "\\\"").replace("\n", "\\n");
  }
}
