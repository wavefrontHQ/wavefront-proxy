package com.wavefront.agent;

import static com.wavefront.agent.ProxyContext.entityPropertiesFactoryMap;
import static com.wavefront.agent.ProxyContext.queuesManager;
import static com.wavefront.agent.TestUtils.*;
import static com.wavefront.sdk.common.Constants.*;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.channel.HealthCheckManagerImpl;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.buffers.BuffersManagerConfig;
import com.wavefront.agent.core.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.core.handlers.ReportableEntityHandler;
import com.wavefront.agent.core.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueuesManager;
import com.wavefront.agent.core.queues.TestQueue;
import com.wavefront.agent.listeners.otlp.OtlpTestHelpers;
import com.wavefront.agent.preprocessor.PreprocessorRuleMetrics;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.preprocessor.SpanAddAnnotationIfNotExistsTransformer;
import com.wavefront.agent.preprocessor.SpanReplaceRegexTransformer;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.agent.tls.NaiveTrustManager;
import com.wavefront.api.agent.AgentConfiguration;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.entities.tracing.sampling.RateSampler;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import javax.annotation.concurrent.NotThreadSafe;
import javax.net.SocketFactory;
import javax.net.ssl.*;
import junit.framework.AssertionFailedError;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.*;
import org.junit.rules.Timeout;
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

@NotThreadSafe
public class PushAgentTest {
  private static SSLSocketFactory sslSocketFactory;
  // Derived RED metrics related.
  private final String PREPROCESSED_APPLICATION_TAG_VALUE = "preprocessedApplication";
  private final String PREPROCESSED_SERVICE_TAG_VALUE = "preprocessedService";
  private final String PREPROCESSED_CLUSTER_TAG_VALUE = "preprocessedCluster";
  private final String PREPROCESSED_SHARD_TAG_VALUE = "preprocessedShard";
  private final String PREPROCESSED_SOURCE_VALUE = "preprocessedSource";
  private final long alignedStartTimeEpochSeconds = System.currentTimeMillis() / 1000 / 60 * 60;
  private PushAgent proxy;
  private ReportableEntityHandler<ReportPoint> mockPointHandler =
      MockReportableEntityHandlerFactory.getMockReportPointHandler();
  private ReportableEntityHandler<ReportSourceTag> mockSourceTagHandler =
      MockReportableEntityHandlerFactory.getMockSourceTagHandler();
  private ReportableEntityHandler<ReportPoint> mockHistogramHandler =
      MockReportableEntityHandlerFactory.getMockHistogramHandler();
  private ReportableEntityHandler<Span> mockTraceHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private ReportableEntityHandler<SpanLogs> mockTraceSpanLogsHandler =
      MockReportableEntityHandlerFactory.getMockTraceSpanLogsHandler();
  private ReportableEntityHandler<ReportEvent> mockEventHandler =
      MockReportableEntityHandlerFactory.getMockEventHandlerImpl();
  private WavefrontSender mockWavefrontSender = EasyMock.createMock(WavefrontSender.class);

  private ReportableEntityHandlerFactory mockHandlerFactory =
      MockReportableEntityHandlerFactory.createMockHandlerFactory(
          mockPointHandler,
          mockSourceTagHandler,
          mockHistogramHandler,
          mockTraceHandler,
          mockTraceSpanLogsHandler,
          mockEventHandler);
  private HttpClient mockHttpClient = EasyMock.createMock(HttpClient.class);

  @Rule public Timeout globalTimeout = Timeout.seconds(5);

  @BeforeClass
  public static void init() throws Exception {
    TrustManager[] tm = new TrustManager[] {new NaiveTrustManager()};
    SSLContext context = SSLContext.getInstance("SSL");
    context.init(new KeyManager[0], tm, new SecureRandom());
    sslSocketFactory = context.getSocketFactory();
    HttpsURLConnection.setDefaultSSLSocketFactory(context.getSocketFactory());
    HttpsURLConnection.setDefaultHostnameVerifier((h, s) -> h.equals("localhost"));

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.disk = false;
    BuffersManager.init(cfg);

    queuesManager =
        new QueuesManager() {
          Map<String, TestQueue> queues = new HashMap<>();

          @Override
          public QueueInfo initQueue(ReportableEntityType entityType) {
            return queues.computeIfAbsent(entityType.toString(), s -> new TestQueue(entityType));
          }
        };
  }

  @Before
  public void setup() throws Exception {
    proxy = new PushAgent();
    proxy.proxyConfig.flushThreads = 2;
    proxy.proxyConfig.disableBuffer = true;
    proxy.proxyConfig.dataDogRequestRelaySyncMode = true;
    proxy.proxyConfig.dataDogProcessSystemMetrics = false;
    proxy.proxyConfig.dataDogProcessServiceChecks = true;
    assertEquals(2, proxy.proxyConfig.getFlushThreads());
    assertFalse(proxy.proxyConfig.isDataDogProcessSystemMetrics());
    assertTrue(proxy.proxyConfig.isDataDogProcessServiceChecks());
  }

  @After
  public void teardown() {
    proxy.shutdown();
  }

  @Test
  public void testSecureAll() throws Exception {
    int securePort1 = findAvailablePort();
    int securePort2 = findAvailablePort();
    proxy.proxyConfig.privateCertPath =
        getClass().getClassLoader().getResource("demo.cert").getPath();
    proxy.proxyConfig.privateKeyPath =
        getClass().getClassLoader().getResource("demo.key").getPath();
    proxy.proxyConfig.tlsPorts = "*";
    proxy.initSslContext();
    proxy.proxyConfig.pushListenerPorts = securePort1 + "," + securePort2;
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D), () -> null);
    proxy.startGraphiteListener(securePort1, mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(securePort2, mockHandlerFactory, null, sampler);
    waitUntilListenerIsOnline(securePort1);
    waitUntilListenerIsOnline(securePort2);
    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test")
            .setHost("test1")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test")
            .setHost("test2")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(1.0d)
            .build());
    expectLastCall();
    replay(mockPointHandler);

    // try plaintext over tcp first
    Socket socket = sslSocketFactory.createSocket("localhost", securePort1);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr =
        "metric.test 0 "
            + alignedStartTimeEpochSeconds
            + " source=test1\n"
            + "metric.test 1 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test2\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockPointHandler);

    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test")
            .setHost("test3")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test")
            .setHost("test4")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(1.0d)
            .build());
    expectLastCall();
    replay(mockPointHandler);

    // secure test
    socket = sslSocketFactory.createSocket("localhost", securePort2);
    stream = new BufferedOutputStream(socket.getOutputStream());
    payloadStr =
        "metric.test 0 "
            + alignedStartTimeEpochSeconds
            + " source=test3\n"
            + "metric.test 1 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test4\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerPlaintextUncompressed() throws Exception {
    int port = findAvailablePort();
    int securePort = findAvailablePort();
    proxy.proxyConfig.privateCertPath =
        getClass().getClassLoader().getResource("demo.cert").getPath();
    proxy.proxyConfig.privateKeyPath =
        getClass().getClassLoader().getResource("demo.key").getPath();
    proxy.proxyConfig.tlsPorts = "1,23 , 4,   , " + securePort + "  ,6";
    proxy.initSslContext();
    proxy.proxyConfig.pushListenerPorts = port + "," + securePort;
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D), () -> null);
    proxy.startGraphiteListener(port, mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(securePort, mockHandlerFactory, null, sampler);
    waitUntilListenerIsOnline(port);
    waitUntilListenerIsOnline(securePort);
    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test")
            .setHost("test1")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test")
            .setHost("test2")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(1.0d)
            .build());
    expectLastCall();
    replay(mockPointHandler);

    // try plaintext over tcp first
    Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr =
        "metric.test 0 "
            + alignedStartTimeEpochSeconds
            + " source=test1\n"
            + "metric.test 1 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test2\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockPointHandler);

    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test")
            .setHost("test3")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test")
            .setHost("test4")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(1.0d)
            .build());
    expectLastCall();
    replay(mockPointHandler);

    // secure test
    socket = sslSocketFactory.createSocket("localhost", securePort);
    stream = new BufferedOutputStream(socket.getOutputStream());
    payloadStr =
        "metric.test 0 "
            + alignedStartTimeEpochSeconds
            + " source=test3\n"
            + "metric.test 1 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test4\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerGzippedPlaintextStream() throws Exception {
    int port = findAvailablePort();
    int securePort = findAvailablePort();
    proxy.proxyConfig.privateCertPath =
        getClass().getClassLoader().getResource("demo.cert").getPath();
    proxy.proxyConfig.privateKeyPath =
        getClass().getClassLoader().getResource("demo.key").getPath();
    proxy.proxyConfig.tlsPorts = "1,23 , 4,   , " + securePort + "  ,6";
    proxy.initSslContext();
    proxy.proxyConfig.pushListenerPorts = port + "," + securePort;
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D), () -> null);
    proxy.startGraphiteListener(port, mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(securePort, mockHandlerFactory, null, sampler);
    waitUntilListenerIsOnline(port);
    waitUntilListenerIsOnline(securePort);
    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric2.test")
            .setHost("test1")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric2.test")
            .setHost("test2")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(1.0d)
            .build());
    expectLastCall();
    replay(mockPointHandler);

    // try gzipped plaintext stream over tcp
    String payloadStr =
        "metric2.test 0 "
            + alignedStartTimeEpochSeconds
            + " source=test1\n"
            + "metric2.test 1 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test2\n";
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
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric2.test")
            .setHost("test3")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric2.test")
            .setHost("test4")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(1.0d)
            .build());
    expectLastCall();
    replay(mockPointHandler);

    // try gzipped plaintext stream over tcp
    payloadStr =
        "metric2.test 0 "
            + alignedStartTimeEpochSeconds
            + " source=test3\n"
            + "metric2.test 1 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test4\n";
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
    int port = findAvailablePort();
    int securePort = findAvailablePort();
    int healthCheckPort = findAvailablePort();
    proxy.proxyConfig.privateCertPath =
        getClass().getClassLoader().getResource("demo.cert").getPath();
    proxy.proxyConfig.privateKeyPath =
        getClass().getClassLoader().getResource("demo.key").getPath();
    proxy.proxyConfig.tlsPorts = "1,23 , 4,   , " + securePort + "  ,6";
    proxy.initSslContext();
    proxy.proxyConfig.pushListenerPorts = port + "," + securePort;
    proxy.proxyConfig.httpHealthCheckPath = "/health";
    proxy.proxyConfig.httpHealthCheckPorts = String.valueOf(healthCheckPort);
    proxy.proxyConfig.httpHealthCheckAllPorts = true;
    proxy.healthCheckManager = new HealthCheckManagerImpl(proxy.proxyConfig);
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D), () -> null);
    proxy.startGraphiteListener(port, mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(securePort, mockHandlerFactory, null, sampler);
    proxy.startHealthCheckListener(healthCheckPort);
    waitUntilListenerIsOnline(port);
    waitUntilListenerIsOnline(securePort);
    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric3.test")
            .setHost("test1")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric3.test")
            .setHost("test2")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(1.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric3.test")
            .setHost("test3")
            .setTimestamp((alignedStartTimeEpochSeconds + 2) * 1000)
            .setValue(2.0d)
            .build());
    expectLastCall();
    replay(mockPointHandler);

    // try http connection
    String payloadStr =
        "metric3.test 0 "
            + alignedStartTimeEpochSeconds
            + " source=test1\n"
            + "metric3.test 1 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test2\n"
            + "metric3.test 2 "
            + (alignedStartTimeEpochSeconds + 2)
            + " source=test3"; // note the lack of newline at the end!
    assertEquals(202, httpPost("http://localhost:" + port, payloadStr));
    assertEquals(200, httpGet("http://localhost:" + port + "/health"));
    assertEquals(202, httpGet("http://localhost:" + port + "/health2"));
    assertEquals(200, httpGet("http://localhost:" + healthCheckPort + "/health"));
    assertEquals(404, httpGet("http://localhost:" + healthCheckPort + "/health2"));
    verify(mockPointHandler);

    // secure test
    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric3.test")
            .setHost("test4")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric3.test")
            .setHost("test5")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(1.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric3.test")
            .setHost("test6")
            .setTimestamp((alignedStartTimeEpochSeconds + 2) * 1000)
            .setValue(2.0d)
            .build());
    expectLastCall();
    replay(mockPointHandler);

    // try http connection
    payloadStr =
        "metric3.test 0 "
            + alignedStartTimeEpochSeconds
            + " source=test4\n"
            + "metric3.test 1 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test5\n"
            + "metric3.test 2 "
            + (alignedStartTimeEpochSeconds + 2)
            + " source=test6"; // note the lack of newline at the end!
    assertEquals(202, httpPost("https://localhost:" + securePort, payloadStr));
    verify(mockPointHandler);
  }

  @Test
  public void testWavefrontUnifiedPortHandlerHttpGzipped() throws Exception {
    int port = findAvailablePort();
    int securePort = findAvailablePort();
    proxy.proxyConfig.privateCertPath =
        getClass().getClassLoader().getResource("demo.cert").getPath();
    proxy.proxyConfig.privateKeyPath =
        getClass().getClassLoader().getResource("demo.key").getPath();
    proxy.proxyConfig.tlsPorts = "1,23 , 4,   , " + securePort + "  ,6";
    proxy.initSslContext();
    proxy.proxyConfig.pushListenerPorts = port + "," + securePort;
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D), () -> null);
    proxy.startGraphiteListener(port, mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(securePort, mockHandlerFactory, null, sampler);
    waitUntilListenerIsOnline(port);
    waitUntilListenerIsOnline(securePort);
    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric4.test")
            .setHost("test1")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric4.test")
            .setHost("test2")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(1.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric4.test")
            .setHost("test3")
            .setTimestamp((alignedStartTimeEpochSeconds + 2) * 1000)
            .setValue(2.0d)
            .build());
    expectLastCall();
    replay(mockPointHandler);

    // try http connection with gzip
    String payloadStr =
        "metric4.test 0 "
            + alignedStartTimeEpochSeconds
            + " source=test1\n"
            + "metric4.test 1 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test2\n"
            + "metric4.test 2 "
            + (alignedStartTimeEpochSeconds + 2)
            + " source=test3"; // note the lack of newline at the end!
    gzippedHttpPost("http://localhost:" + port, payloadStr);
    verify(mockPointHandler);

    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric_4.test")
            .setHost("test1")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric_4.test")
            .setHost("test2")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(1.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric_4.test")
            .setHost("test3")
            .setTimestamp((alignedStartTimeEpochSeconds + 2) * 1000)
            .setValue(2.0d)
            .build());
    expectLastCall();
    replay(mockPointHandler);

    // try secure http connection with gzip
    payloadStr =
        "metric_4.test 0 "
            + alignedStartTimeEpochSeconds
            + " source=test1\n"
            + "metric_4.test 1 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test2\n"
            + "metric_4.test 2 "
            + (alignedStartTimeEpochSeconds + 2)
            + " source=test3"; // note the lack of newline at the end!
    gzippedHttpPost("https://localhost:" + securePort, payloadStr);
    verify(mockPointHandler);
  }

  // test that histograms received on Wavefront port get routed to the correct
  // handler
  @Test
  public void testHistogramDataOnWavefrontUnifiedPortHandlerPlaintextUncompressed()
      throws Exception {
    int port = findAvailablePort();
    proxy.startGraphiteListener(
        port, mockHandlerFactory, null, new SpanSampler(new RateSampler(1.0D), () -> null));
    waitUntilListenerIsOnline(port);
    reset(mockHistogramHandler);
    mockHistogramHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test.histo")
            .setHost("test1")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(
                Histogram.newBuilder()
                    .setType(HistogramType.TDIGEST)
                    .setDuration(60000)
                    .setBins(ImmutableList.of(10.0d, 100.0d))
                    .setCounts(ImmutableList.of(5, 10))
                    .build())
            .build());
    mockHistogramHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test.histo")
            .setHost("test2")
            .setTimestamp((alignedStartTimeEpochSeconds + 60) * 1000)
            .setValue(
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
    String payloadStr =
        "!M "
            + alignedStartTimeEpochSeconds
            + " #5 10.0 #10 100.0 metric.test.histo source=test1\n"
            + "!M "
            + (alignedStartTimeEpochSeconds + 60)
            + " #5 20.0 #6 30.0 #7 40.0 metric.test.histo source=test2\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockHistogramHandler);
  }

  // test Wavefront port handler with mixed payload: metrics, histograms, source
  // tags
  @Test
  public void testWavefrontUnifiedPortHandlerPlaintextUncompressedMixedDataPayload()
      throws Exception {
    int port = findAvailablePort();
    proxy.proxyConfig.pushListenerPorts = String.valueOf(port);
    proxy.startGraphiteListener(
        Integer.parseInt(proxy.proxyConfig.getPushListenerPorts()),
        mockHandlerFactory,
        null,
        new SpanSampler(new RateSampler(1.0D), () -> null));
    waitUntilListenerIsOnline(port);
    reset(mockHistogramHandler);
    reset(mockPointHandler);
    reset(mockSourceTagHandler);
    reset(mockEventHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test.mixed")
            .setHost("test2")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(10d)
            .build());
    mockHistogramHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test.mixed")
            .setHost("test1")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(
                Histogram.newBuilder()
                    .setType(HistogramType.TDIGEST)
                    .setDuration(60000)
                    .setBins(ImmutableList.of(10.0d, 100.0d))
                    .setCounts(ImmutableList.of(5, 10))
                    .build())
            .build());
    mockEventHandler.report(
        ReportEvent.newBuilder()
            .setStartTime(alignedStartTimeEpochSeconds * 1000)
            .setEndTime(alignedStartTimeEpochSeconds * 1000 + 1)
            .setName("Event name for testing")
            .setHosts(ImmutableList.of("host1", "host2"))
            .setDimensions(ImmutableMap.of("multi", ImmutableList.of("bar", "baz")))
            .setAnnotations(ImmutableMap.of("severity", "INFO"))
            .setTags(ImmutableList.of("tag1"))
            .build());
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test.mixed")
            .setHost("test2")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(9d)
            .build());
    mockSourceTagHandler.report(
        ReportSourceTag.newBuilder()
            .setOperation(SourceOperationType.SOURCE_TAG)
            .setAction(SourceTagAction.SAVE)
            .setSource("testSource")
            .setAnnotations(ImmutableList.of("newtag1", "newtag2"))
            .build());
    expectLastCall();
    replay(mockPointHandler);
    replay(mockHistogramHandler);
    replay(mockEventHandler);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr =
        "metric.test.mixed 10.0 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test2\n"
            + "!M "
            + alignedStartTimeEpochSeconds
            + " #5 10.0 #10 100.0 metric.test.mixed source=test1\n"
            + "@SourceTag action=save source=testSource newtag1 newtag2\n"
            + "metric.test.mixed 9.0 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test2\n"
            + "@Event "
            + alignedStartTimeEpochSeconds
            + " \"Event name for testing\" host=host1 host=host2 tag=tag1 "
            + "severity=INFO multi=bar multi=baz\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockPointHandler, mockHistogramHandler, mockEventHandler);
  }

  @Test
  public void testTraceUnifiedPortHandlerPlaintextDebugSampling() throws Exception {
    int tracePort = findAvailablePort();
    proxy.startTraceListener(
        tracePort, mockHandlerFactory, new SpanSampler(new RateSampler(0.0D), () -> null));
    waitUntilListenerIsOnline(tracePort);
    reset(mockTraceHandler);
    reset(mockTraceSpanLogsHandler);
    String traceId = UUID.randomUUID().toString();
    long timestamp1 = alignedStartTimeEpochSeconds * 1000000 + 12345;
    long timestamp2 = alignedStartTimeEpochSeconds * 1000000 + 23456;
    String spanData =
        "testSpanName parent=parent1 source=testsource spanId=testspanid "
            + "traceId=\""
            + traceId
            + "\" debug=true "
            + alignedStartTimeEpochSeconds
            + " "
            + (alignedStartTimeEpochSeconds + 1)
            + "\n";
    mockTraceSpanLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("dummy")
            .setTraceId(traceId)
            .setSpanId("testspanid")
            .setSpan("_sampledByPolicy=NONE")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp1)
                        .setFields(ImmutableMap.of("key", "value", "key2", "value2"))
                        .build(),
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp2)
                        .setFields(ImmutableMap.of("key3", "value3", "key4", "value4"))
                        .build()))
            .build());
    expectLastCall();
    mockTraceSpanLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("dummy")
            .setTraceId(traceId)
            .setSpanId("testspanid")
            .setSpan("_sampledByPolicy=NONE")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp1)
                        .setFields(ImmutableMap.of("key", "value", "key2", "value2"))
                        .build(),
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp2)
                        .setFields(ImmutableMap.of("key3", "value3", "key4", "value4"))
                        .build()))
            .build());
    expectLastCall();
    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(alignedStartTimeEpochSeconds * 1000)
            .setDuration(1000)
            .setName("testSpanName")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(traceId)
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("parent", "parent1"), new Annotation("debug", "true")))
            .build());
    expectLastCall();
    replay(mockTraceHandler);
    replay(mockTraceSpanLogsHandler);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", tracePort);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr =
        spanData
            + "{\"spanId\":\"testspanid\",\"traceId\":\""
            + traceId
            + "\",\"logs\":[{\"timestamp\":"
            + timestamp1
            + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n"
            + "{\"spanId\":\"testspanid\",\"traceId\":\""
            + traceId
            + "\",\"logs\":[{\"timestamp\":"
            + timestamp1
            + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}],"
            + "\"span\":\""
            + escapeSpanData(spanData)
            + "\"}\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockTraceHandler, mockTraceSpanLogsHandler);
  }

  @Test
  public void testTraceUnifiedPortHandlerPlaintext() throws Exception {
    int tracePort = findAvailablePort();
    proxy.proxyConfig.traceListenerPorts = String.valueOf(tracePort);
    proxy.startTraceListener(
        Integer.parseInt(proxy.proxyConfig.getTraceListenerPorts()),
        mockHandlerFactory,
        new SpanSampler(new RateSampler(1.0D), () -> null));
    waitUntilListenerIsOnline(tracePort);
    reset(mockTraceHandler);
    reset(mockTraceSpanLogsHandler);
    String traceId = UUID.randomUUID().toString();
    long timestamp1 = alignedStartTimeEpochSeconds * 1000000 + 12345;
    long timestamp2 = alignedStartTimeEpochSeconds * 1000000 + 23456;
    String spanData =
        "testSpanName parent=parent1 source=testsource spanId=testspanid "
            + "traceId=\""
            + traceId
            + "\" parent=parent2 "
            + alignedStartTimeEpochSeconds
            + " "
            + (alignedStartTimeEpochSeconds + 1)
            + "\n";
    mockTraceSpanLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("dummy")
            .setTraceId(traceId)
            .setSpanId("testspanid")
            .setSpan("_sampledByPolicy=NONE")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp1)
                        .setFields(ImmutableMap.of("key", "value", "key2", "value2"))
                        .build(),
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp2)
                        .setFields(ImmutableMap.of("key3", "value3", "key4", "value4"))
                        .build()))
            .build());
    expectLastCall();
    mockTraceSpanLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("dummy")
            .setTraceId(traceId)
            .setSpanId("testspanid")
            .setSpan("_sampledByPolicy=NONE")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp1)
                        .setFields(ImmutableMap.of("key", "value", "key2", "value2"))
                        .build(),
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp2)
                        .setFields(ImmutableMap.of("key3", "value3", "key4", "value4"))
                        .build()))
            .build());
    expectLastCall();
    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(alignedStartTimeEpochSeconds * 1000)
            .setDuration(1000)
            .setName("testSpanName")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(traceId)
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("parent", "parent1"), new Annotation("parent", "parent2")))
            .build());
    expectLastCall();
    replay(mockTraceHandler);
    replay(mockTraceSpanLogsHandler);

    Socket socket = SocketFactory.getDefault().createSocket("localhost", tracePort);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr =
        spanData
            + "{\"spanId\":\"testspanid\",\"traceId\":\""
            + traceId
            + "\",\"logs\":[{\"timestamp\":"
            + timestamp1
            + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n"
            + "{\"spanId\":\"testspanid\",\"traceId\":\""
            + traceId
            + "\",\"logs\":[{\"timestamp\":"
            + timestamp1
            + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}],"
            + "\"span\":\""
            + escapeSpanData(spanData)
            + "\"}\n";
    stream.write(payloadStr.getBytes());
    stream.flush();
    socket.close();
    verifyWithTimeout(500, mockTraceHandler, mockTraceSpanLogsHandler);
  }

  @Test
  public void testCustomTraceUnifiedPortHandlerDerivedMetrics() throws Exception {
    int customTracePort = findAvailablePort();
    proxy.proxyConfig.customTracingListenerPorts = String.valueOf(customTracePort);
    setUserPreprocessorForTraceDerivedREDMetrics(customTracePort);
    proxy.startCustomTracingListener(
        Integer.parseInt(proxy.proxyConfig.getCustomTracingListenerPorts()),
        mockHandlerFactory,
        mockWavefrontSender,
        new SpanSampler(new RateSampler(1.0D), () -> null));
    waitUntilListenerIsOnline(customTracePort);
    reset(mockTraceHandler);
    reset(mockWavefrontSender);

    String traceId = UUID.randomUUID().toString();
    String spanData =
        "testSpanName source=testsource spanId=testspanid "
            + "traceId=\""
            + traceId
            + "\" "
            + alignedStartTimeEpochSeconds
            + " "
            + (alignedStartTimeEpochSeconds + 1)
            + "\n";

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(alignedStartTimeEpochSeconds * 1000)
            .setDuration(1000)
            .setName("testSpanName")
            .setSource(PREPROCESSED_SOURCE_VALUE)
            .setSpanId("testspanid")
            .setTraceId(traceId)
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("application", PREPROCESSED_APPLICATION_TAG_VALUE),
                    new Annotation("service", PREPROCESSED_SERVICE_TAG_VALUE),
                    new Annotation("cluster", PREPROCESSED_CLUSTER_TAG_VALUE),
                    new Annotation("shard", PREPROCESSED_SHARD_TAG_VALUE)))
            .build());
    expectLastCall();

    Capture<HashMap<String, String>> tagsCapture = EasyMock.newCapture();
    mockWavefrontSender.sendMetric(
        eq(HEART_BEAT_METRIC),
        eq(1.0),
        anyLong(),
        eq(PREPROCESSED_SOURCE_VALUE),
        EasyMock.capture(tagsCapture));
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
    PreprocessorRuleMetrics preprocessorRuleMetrics = new PreprocessorRuleMetrics(null, null, null);
    preprocessor
        .forSpan()
        .addTransformer(
            new SpanAddAnnotationIfNotExistsTransformer(
                "application",
                PREPROCESSED_APPLICATION_TAG_VALUE,
                x -> true,
                preprocessorRuleMetrics));
    preprocessor
        .forSpan()
        .addTransformer(
            new SpanAddAnnotationIfNotExistsTransformer(
                "service", PREPROCESSED_SERVICE_TAG_VALUE, x -> true, preprocessorRuleMetrics));
    preprocessor
        .forSpan()
        .addTransformer(
            new SpanAddAnnotationIfNotExistsTransformer(
                "cluster", PREPROCESSED_CLUSTER_TAG_VALUE, x -> true, preprocessorRuleMetrics));
    preprocessor
        .forSpan()
        .addTransformer(
            new SpanAddAnnotationIfNotExistsTransformer(
                "shard", PREPROCESSED_SHARD_TAG_VALUE, x -> true, preprocessorRuleMetrics));
    preprocessor
        .forSpan()
        .addTransformer(
            new SpanReplaceRegexTransformer(
                "sourceName",
                "^test.*",
                PREPROCESSED_SOURCE_VALUE,
                null,
                null,
                false,
                x -> true,
                preprocessorRuleMetrics));
    Map<Integer, ReportableEntityPreprocessor> userPreprocessorMap = new HashMap<>();
    userPreprocessorMap.put(port, preprocessor);
    proxy.preprocessors.userPreprocessors = userPreprocessorMap;
  }

  @Test
  public void testCustomTraceUnifiedPortHandlerPlaintext() throws Exception {
    int customTracePort = findAvailablePort();
    proxy.startCustomTracingListener(
        customTracePort,
        mockHandlerFactory,
        mockWavefrontSender,
        new SpanSampler(new RateSampler(1.0D), () -> null));
    waitUntilListenerIsOnline(customTracePort);
    reset(mockTraceHandler);
    reset(mockTraceSpanLogsHandler);
    reset(mockWavefrontSender);
    String traceId = UUID.randomUUID().toString();
    long timestamp1 = alignedStartTimeEpochSeconds * 1000000 + 12345;
    long timestamp2 = alignedStartTimeEpochSeconds * 1000000 + 23456;
    String spanData =
        "testSpanName source=testsource spanId=testspanid "
            + "traceId=\""
            + traceId
            + "\" application=application1 service=service1 "
            + alignedStartTimeEpochSeconds
            + " "
            + (alignedStartTimeEpochSeconds + 1)
            + "\n";
    mockTraceSpanLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("dummy")
            .setTraceId(traceId)
            .setSpanId("testspanid")
            .setSpan("_sampledByPolicy=NONE")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp1)
                        .setFields(ImmutableMap.of("key", "value", "key2", "value2"))
                        .build(),
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp2)
                        .setFields(ImmutableMap.of("key3", "value3", "key4", "value4"))
                        .build()))
            .build());
    expectLastCall();
    mockTraceSpanLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("dummy")
            .setTraceId(traceId)
            .setSpanId("testspanid")
            .setSpan("_sampledByPolicy=NONE")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp1)
                        .setFields(ImmutableMap.of("key", "value", "key2", "value2"))
                        .build(),
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp2)
                        .setFields(ImmutableMap.of("key3", "value3", "key4", "value4"))
                        .build()))
            .build());
    expectLastCall();
    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(alignedStartTimeEpochSeconds * 1000)
            .setDuration(1000)
            .setName("testSpanName")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(traceId)
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("application", "application1"),
                    new Annotation("service", "service1")))
            .build());
    expectLastCall();
    Capture<HashMap<String, String>> tagsCapture = EasyMock.newCapture();
    mockWavefrontSender.sendMetric(
        eq(HEART_BEAT_METRIC), eq(1.0), anyLong(), eq("testsource"), EasyMock.capture(tagsCapture));
    EasyMock.expectLastCall().anyTimes();
    replay(mockTraceHandler, mockTraceSpanLogsHandler, mockWavefrontSender);
    Socket socket = SocketFactory.getDefault().createSocket("localhost", customTracePort);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String payloadStr =
        spanData
            + "{\"spanId\":\"testspanid\",\"traceId\":\""
            + traceId
            + "\",\"logs\":[{\"timestamp\":"
            + timestamp1
            + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n"
            + "{\"spanId\":\"testspanid\",\"traceId\":\""
            + traceId
            + "\",\"logs\":[{\"timestamp\":"
            + timestamp1
            + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}],"
            + "\"span\":\""
            + escapeSpanData(spanData)
            + "\"}\n";
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
    int ddPort = findAvailablePort();
    proxy.startDataDogListener(ddPort, mockHandlerFactory, mockHttpClient);
    int ddPort2 = findAvailablePort();
    PushAgent proxy2 = new PushAgent();
    proxy2.proxyConfig.flushThreads = 2;
    proxy2.proxyConfig.dataDogJsonPorts = String.valueOf(ddPort2);
    proxy2.proxyConfig.dataDogRequestRelaySyncMode = true;
    proxy2.proxyConfig.dataDogProcessSystemMetrics = true;
    proxy2.proxyConfig.dataDogProcessServiceChecks = false;
    proxy2.proxyConfig.dataDogRequestRelayTarget = "http://relay-to:1234";
    assertEquals(2, proxy2.proxyConfig.getFlushThreads());
    assertTrue(proxy2.proxyConfig.isDataDogProcessSystemMetrics());
    assertFalse(proxy2.proxyConfig.isDataDogProcessServiceChecks());

    proxy2.startDataDogListener(
        Integer.parseInt(proxy2.proxyConfig.getDataDogJsonPorts()),
        mockHandlerFactory,
        mockHttpClient);
    waitUntilListenerIsOnline(ddPort2);

    int ddPort3 = findAvailablePort();
    PushAgent proxy3 = new PushAgent();
    proxy3.proxyConfig.dataDogProcessSystemMetrics = true;
    proxy3.proxyConfig.dataDogProcessServiceChecks = true;
    assertTrue(proxy3.proxyConfig.isDataDogProcessSystemMetrics());
    assertTrue(proxy3.proxyConfig.isDataDogProcessServiceChecks());

    proxy3.startDataDogListener(ddPort3, mockHandlerFactory, mockHttpClient);
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

    // test 3: post to /intake with system metrics enabled and http relay enabled,
    // but remote
    // unavailable
    reset(mockPointHandler, mockHttpClient, mockHttpResponse, mockStatusLine);
    expect(mockStatusLine.getStatusCode()).andReturn(404); // remote returns a error http code
    expect(mockHttpResponse.getStatusLine()).andReturn(mockStatusLine);
    expect(mockHttpResponse.getEntity()).andReturn(new StringEntity(""));
    expect(mockHttpClient.execute(anyObject(HttpPost.class))).andReturn(mockHttpResponse);
    mockPointHandler.report(anyObject());
    expectLastCall()
        .andThrow(new AssertionFailedError())
        .anyTimes(); // we are not supposed to actually process data!
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
    expectLastCall()
        .andThrow(new AssertionFailedError())
        .anyTimes(); // we are not supposed to actually process data!
    replay(mockHttpClient, mockHttpResponse, mockStatusLine, mockPointHandler);
    gzippedHttpPost(
        "http://localhost:" + ddPort2 + "/api/v1/check_run",
        getResource("ddTestServiceCheck.json"));
    verify(mockHttpClient, mockPointHandler);

    // test 5: post to /api/v1/check_run with service checks enabled
    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("testApp.status")
            .setHost("testhost")
            .setTimestamp(1536719228000L)
            .setValue(3.0d)
            .build());
    expectLastCall().once();
    replay(mockPointHandler);
    gzippedHttpPost(
        "http://localhost:" + ddPort + "/api/v1/check_run", getResource("ddTestServiceCheck.json"));
    verify(mockPointHandler);

    // test 6: post to /api/v1/series including a /api/v1/intake call to ensure
    // system host-tags are
    // propogated
    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("system.net.tcp.retrans_segs")
            .setHost("testhost")
            .setTimestamp(1531176936000L)
            .setValue(0.0d)
            .setAnnotations(ImmutableMap.of("app", "closedstack", "role", "control"))
            .build());
    expectLastCall().once();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("system.net.tcp.listen_drops")
            .setHost("testhost")
            .setTimestamp(1531176936000L)
            .setValue(0.0d)
            .setAnnotations(
                ImmutableMap.of(
                    "_source", "Launcher", "env", "prod", "app", "openstack", "role", "control"))
            .build());
    expectLastCall().once();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("system.net.packets_in.count")
            .setHost("testhost")
            .setTimestamp(1531176936000L)
            .setValue(12.052631578947368d)
            .setAnnotations(
                ImmutableMap.of("device", "eth0", "app", "closedstack", "role", "control"))
            .build());
    expectLastCall().once();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("test.metric")
            .setHost("testhost")
            .setTimestamp(1531176936000L)
            .setValue(400.0d)
            .setAnnotations(ImmutableMap.of("app", "closedstack", "role", "control"))
            .build());
    expectLastCall().once();
    replay(mockPointHandler);
    gzippedHttpPost(
        "http://localhost:" + ddPort3 + "/intake", getResource("ddTestSystemMetadataOnly.json"));
    gzippedHttpPost(
        "http://localhost:" + ddPort3 + "/api/v1/series", getResource("ddTestTimeseries.json"));
    verify(mockPointHandler);

    // test 7: post multiple checks to /api/v1/check_run with service checks enabled
    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("testApp.status")
            .setHost("testhost")
            .setTimestamp(1536719228000L)
            .setValue(3.0d)
            .build());
    expectLastCall().once();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("testApp2.status")
            .setHost("testhost2")
            .setTimestamp(1536719228000L)
            .setValue(2.0d)
            .build());
    expectLastCall().once();
    replay(mockPointHandler);
    gzippedHttpPost(
        "http://localhost:" + ddPort + "/api/v1/check_run",
        getResource("ddTestMultipleServiceChecks.json"));
    verify(mockPointHandler);
  }

  // @Test
  // public void testDeltaCounterHandlerMixedData() throws Exception {
  // moved to HttpEndToEndTest.testEndToEndDelta
  // }

  //  @Test
  //  public void testDeltaCounterHandlerDataStream() throws Exception {
  // SEE HttpEndToEndTest.testEndToEndDelta
  //  }

  @Test
  public void testOpenTSDBPortHandler() throws Exception {
    int port = findAvailablePort();
    proxy.startOpenTsdbListener(port, mockHandlerFactory);
    waitUntilListenerIsOnline(port);
    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric4.test")
            .setHost("test1")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setAnnotations(ImmutableMap.of("env", "prod"))
            .setValue(0.0d)
            .build());
    expectLastCall().times(2);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric4.test")
            .setHost("test2")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setAnnotations(ImmutableMap.of("env", "prod"))
            .setValue(1.0d)
            .build());
    expectLastCall().times(2);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric4.test")
            .setHost("test3")
            .setTimestamp((alignedStartTimeEpochSeconds + 2) * 1000)
            .setAnnotations(ImmutableMap.of("env", "prod"))
            .setValue(2.0d)
            .build());
    expectLastCall().times(2);
    mockPointHandler.reject((ReportPoint) eq(null), anyString());
    expectLastCall().once();
    replay(mockPointHandler);

    String payloadStr =
        "[\n"
            + "  {\n"
            + "    \"metric\": \"metric4.test\",\n"
            + "    \"timestamp\": "
            + alignedStartTimeEpochSeconds
            + ",\n"
            + "    \"value\": 0.0,\n"
            + "    \"tags\": {\n"
            + "      \"host\": \"test1\",\n"
            + "      \"env\": \"prod\"\n"
            + "    }\n"
            + "  },\n"
            + "  {\n"
            + "    \"metric\": \"metric4.test\",\n"
            + "    \"timestamp\": "
            + (alignedStartTimeEpochSeconds + 1)
            + ",\n"
            + "    \"value\": 1.0,\n"
            + "    \"tags\": {\n"
            + "      \"host\": \"test2\",\n"
            + "      \"env\": \"prod\"\n"
            + "    }\n"
            + "  }\n"
            + "]\n";
    String payloadStr2 =
        "[\n"
            + "  {\n"
            + "    \"metric\": \"metric4.test\",\n"
            + "    \"timestamp\": "
            + (alignedStartTimeEpochSeconds + 2)
            + ",\n"
            + "    \"value\": 2.0,\n"
            + "    \"tags\": {\n"
            + "      \"host\": \"test3\",\n"
            + "      \"env\": \"prod\"\n"
            + "    }\n"
            + "  },\n"
            + "  {\n"
            + "    \"metric\": \"metric4.test\",\n"
            + "    \"timestamp\": "
            + alignedStartTimeEpochSeconds
            + ",\n"
            + "    \"tags\": {\n"
            + "      \"host\": \"test4\",\n"
            + "      \"env\": \"prod\"\n"
            + "    }\n"
            + "  }]\n";

    Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
    BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream());
    String points =
        "version\n"
            + "put metric4.test "
            + alignedStartTimeEpochSeconds
            + " 0 host=test1 env=prod\n"
            + "put metric4.test "
            + (alignedStartTimeEpochSeconds + 1)
            + " 1 host=test2 env=prod\n"
            + "put metric4.test "
            + (alignedStartTimeEpochSeconds + 2)
            + " 2 host=test3 env=prod\n";
    stream.write(points.getBytes());
    stream.flush();
    socket.close();

    // nonexistent path should return 400
    assertEquals(400, gzippedHttpPost("http://localhost:" + port + "/api/nonexistent", ""));
    assertEquals(200, gzippedHttpPost("http://localhost:" + port + "/api/version", ""));
    // malformed json should return 400
    assertEquals(400, gzippedHttpPost("http://localhost:" + port + "/api/put", "{]"));
    assertEquals(204, gzippedHttpPost("http://localhost:" + port + "/api/put", payloadStr));
    // 1 good, 1 invalid point - should return 400, but good point should still go
    // through
    assertEquals(400, gzippedHttpPost("http://localhost:" + port + "/api/put", payloadStr2));

    verify(mockPointHandler);
  }

  @Test
  public void testJsonMetricsPortHandler() throws Exception {
    int port = findAvailablePort();
    proxy.proxyConfig.jsonListenerPorts = String.valueOf(port);
    proxy.startJsonListener(port, mockHandlerFactory);
    waitUntilListenerIsOnline(port);
    reset(mockPointHandler);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test")
            .setHost("testSource")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setAnnotations(ImmutableMap.of("env", "prod", "dc", "test1"))
            .setValue(1.0d)
            .build());
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test.cpu.usage.idle")
            .setHost("testSource")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(99.0d)
            .build());
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test.cpu.usage.user")
            .setHost("testSource")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.5d)
            .build());
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test.cpu.usage.system")
            .setHost("testSource")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.7d)
            .build());
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test.disk.free")
            .setHost("testSource")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test.mem.used")
            .setHost("testSource")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(50.0d)
            .build());
    replay(mockPointHandler);

    String payloadStr =
        "{\n"
            + "  \"value\": 1.0,\n"
            + "  \"tags\": {\n"
            + "    \"dc\": \"test1\",\n"
            + "    \"env\": \"prod\"\n"
            + "  }\n"
            + "}\n";
    String payloadStr2 =
        "{\n"
            + "  \"cpu.usage\": {\n"
            + "    \"idle\": 99.0,\n"
            + "    \"user\": 0.5,\n"
            + "    \"system\": 0.7\n"
            + "  },\n"
            + "  \"disk.free\": 0.0,\n"
            + "  \"mem\": {\n"
            + "    \"used\": 50.0\n"
            + "  }\n"
            + "}\n";
    assertEquals(
        200,
        gzippedHttpPost(
            "http://localhost:"
                + port
                + "/?h=testSource&p=metric.test&"
                + "d="
                + alignedStartTimeEpochSeconds * 1000,
            payloadStr));
    assertEquals(
        200,
        gzippedHttpPost(
            "http://localhost:"
                + port
                + "/?h=testSource&p=metric.test&"
                + "d="
                + alignedStartTimeEpochSeconds * 1000,
            payloadStr2));
    verify(mockPointHandler);
  }

  @Test
  public void testOtlpHttpPortHandlerTraces() throws Exception {
    int port = findAvailablePort();
    proxy.proxyConfig.hostname = "defaultLocalHost";
    SpanSampler mockSampler = EasyMock.createMock(SpanSampler.class);
    proxy.startOtlpHttpListener(port, mockHandlerFactory, mockWavefrontSender, mockSampler);
    waitUntilListenerIsOnline(port);

    reset(mockSampler, mockTraceHandler, mockTraceSpanLogsHandler, mockWavefrontSender);
    expect(mockSampler.sample(anyObject(), anyObject())).andReturn(true);
    Capture<wavefront.report.Span> actualSpan = EasyMock.newCapture();
    Capture<wavefront.report.SpanLogs> actualLogs = EasyMock.newCapture();
    mockTraceHandler.report(capture(actualSpan));
    mockTraceSpanLogsHandler.report(capture(actualLogs));

    replay(mockSampler, mockTraceHandler, mockTraceSpanLogsHandler);

    io.opentelemetry.proto.trace.v1.Span.Event otlpEvent = OtlpTestHelpers.otlpSpanEvent(0);
    io.opentelemetry.proto.trace.v1.Span otlpSpan =
        OtlpTestHelpers.otlpSpanGenerator().addEvents(otlpEvent).build();
    ExportTraceServiceRequest otlpRequest = OtlpTestHelpers.otlpTraceRequest(otlpSpan);

    String validUrl = "http://localhost:" + port + "/v1/traces";
    assertEquals(200, httpPost(validUrl, otlpRequest.toByteArray(), "application/x-protobuf"));
    assertEquals(400, httpPost(validUrl, "junk".getBytes(), "application/x-protobuf"));
    assertEquals(
        400,
        httpPost(
            "http://localhost:" + port + "/unknown",
            otlpRequest.toByteArray(),
            "application/x-protobuf"));
    verify(mockSampler, mockTraceHandler, mockTraceSpanLogsHandler);

    Span expectedSpan =
        OtlpTestHelpers.wfSpanGenerator(Arrays.asList(new Annotation("_spanLogs", "true")))
            .setSource("defaultLocalHost")
            .build();
    SpanLogs expectedLogs =
        OtlpTestHelpers.wfSpanLogsGenerator(expectedSpan, 0, "_sampledByPolicy=NONE").build();

    OtlpTestHelpers.assertWFSpanEquals(expectedSpan, actualSpan.getValue());
    assertEquals(expectedLogs, actualLogs.getValue());
  }

  @Test
  public void testOtlpHttpPortHandlerMetrics() throws Exception {
    int port = findAvailablePort();
    proxy.proxyConfig.hostname = "defaultLocalHost";
    proxy.startOtlpHttpListener(port, mockHandlerFactory, null, null);
    waitUntilListenerIsOnline(port);

    reset(mockPointHandler);

    mockPointHandler.report(
        OtlpTestHelpers.wfReportPointGenerator()
            .setMetric("test-gauge")
            .setTimestamp(TimeUnit.SECONDS.toMillis(alignedStartTimeEpochSeconds))
            .setValue(2.3)
            .setHost("defaultLocalHost")
            .build());
    expectLastCall();

    replay(mockPointHandler);
    io.opentelemetry.proto.metrics.v1.Metric simpleGauge =
        OtlpTestHelpers.otlpMetricGenerator()
            .setName("test-gauge")
            .setGauge(
                Gauge.newBuilder()
                    .addDataPoints(
                        NumberDataPoint.newBuilder()
                            .setAsDouble(2.3)
                            .setTimeUnixNano(TimeUnit.SECONDS.toNanos(alignedStartTimeEpochSeconds))
                            .build()))
            .build();
    ExportMetricsServiceRequest payload =
        ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                ResourceMetrics.newBuilder()
                    .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(simpleGauge).build())
                    .build())
            .build();

    String validUrl = "http://localhost:" + port + "/v1/metrics";
    String invalidUrl = "http://localhost:" + port + "/blah";
    assertEquals(400, httpPost(validUrl, "invalid payload".getBytes(), "application/x-protobuf"));
    assertEquals(200, httpPost(validUrl, payload.toByteArray(), "application/x-protobuf"));
    assertEquals(400, httpPost(invalidUrl, payload.toByteArray(), "application/x-protobuf"));
    verify(mockPointHandler);
  }

  @Test
  public void testOtlpGrpcHandlerCanListen() throws Exception {
    int port = findAvailablePort();
    SpanSampler mockSampler = EasyMock.createMock(SpanSampler.class);
    proxy.startOtlpGrpcListener(port, mockHandlerFactory, mockWavefrontSender, mockSampler);
    waitUntilListenerIsOnline(port);
  }

  @Test
  public void testJaegerGrpcHandlerCanListen() throws Exception {
    int port = findAvailablePort();
    SpanSampler mockSampler = EasyMock.createMock(SpanSampler.class);
    proxy.startTraceJaegerGrpcListener(port, mockHandlerFactory, mockWavefrontSender, mockSampler);
    waitUntilListenerIsOnline(port);
  }

  @Test
  public void testWriteHttpJsonMetricsPortHandler() throws Exception {
    int port = findAvailablePort();
    proxy.proxyConfig.writeHttpJsonListenerPorts = String.valueOf(port);
    proxy.proxyConfig.hostname = "defaultLocalHost";
    proxy.startWriteHttpJsonListener(port, mockHandlerFactory);
    waitUntilListenerIsOnline(port);
    reset(mockPointHandler);
    mockPointHandler.reject((ReportPoint) eq(null), anyString());
    expectLastCall().times(2);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("disk.sda.disk_octets.read")
            .setHost("testSource")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(197141504)
            .build());
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("disk.sda.disk_octets.write")
            .setHost("testSource")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(175136768)
            .build());
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("disk.sda.disk_octets.read")
            .setHost("defaultLocalHost")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(297141504)
            .build());
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("disk.sda.disk_octets.write")
            .setHost("defaultLocalHost")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(275136768)
            .build());
    replay(mockPointHandler);
    String payloadStr =
        "[{\n"
            + "\"values\": [197141504, 175136768],\n"
            + "\"dstypes\": [\"counter\", \"counter\"],\n"
            + "\"dsnames\": [\"read\", \"write\"],\n"
            + "\"time\": "
            + alignedStartTimeEpochSeconds
            + ",\n"
            + "\"interval\": 10,\n"
            + "\"host\": \"testSource\",\n"
            + "\"plugin\": \"disk\",\n"
            + "\"plugin_instance\": \"sda\",\n"
            + "\"type\": \"disk_octets\",\n"
            + "\"type_instance\": \"\"\n"
            + "},{\n"
            + "\"values\": [297141504, 275136768],\n"
            + "\"dstypes\": [\"counter\", \"counter\"],\n"
            + "\"dsnames\": [\"read\", \"write\"],\n"
            + "\"time\": "
            + alignedStartTimeEpochSeconds
            + ",\n"
            + "\"interval\": 10,\n"
            + "\"plugin\": \"disk\",\n"
            + "\"plugin_instance\": \"sda\",\n"
            + "\"type\": \"disk_octets\",\n"
            + "\"type_instance\": \"\"\n"
            + "},{\n"
            + "\"dstypes\": [\"counter\", \"counter\"],\n"
            + "\"dsnames\": [\"read\", \"write\"],\n"
            + "\"time\": "
            + alignedStartTimeEpochSeconds
            + ",\n"
            + "\"interval\": 10,\n"
            + "\"plugin\": \"disk\",\n"
            + "\"plugin_instance\": \"sda\",\n"
            + "\"type\": \"disk_octets\",\n"
            + "\"type_instance\": \"\"\n"
            + "}]\n";

    // should be an array
    assertEquals(400, gzippedHttpPost("http://localhost:" + port, "{}"));
    assertEquals(200, gzippedHttpPost("http://localhost:" + port, payloadStr));
    verify(mockPointHandler);
  }

  @Test
  public void testRelayPortHandlerGzipped() throws Exception {
    int port = findAvailablePort();
    proxy.proxyConfig.pushRelayListenerPorts = String.valueOf(port);
    proxy.proxyConfig.pushRelayHistogramAggregator = true;
    proxy.proxyConfig.pushRelayHistogramAggregatorAccumulatorSize = 10L;
    proxy.proxyConfig.pushRelayHistogramAggregatorFlushSecs = 1;
    proxy.startRelayListener(port, mockHandlerFactory, null);
    waitUntilListenerIsOnline(port);
    reset(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler);
    String traceId = UUID.randomUUID().toString();
    long timestamp1 = alignedStartTimeEpochSeconds * 1000000 + 12345;
    long timestamp2 = alignedStartTimeEpochSeconds * 1000000 + 23456;
    String spanData =
        "testSpanName parent=parent1 source=testsource spanId=testspanid "
            + "traceId=\""
            + traceId
            + "\" parent=parent2 "
            + alignedStartTimeEpochSeconds
            + " "
            + (alignedStartTimeEpochSeconds + 1);
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric4.test")
            .setHost("test1")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(0.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric4.test")
            .setHost("test2")
            .setTimestamp((alignedStartTimeEpochSeconds + 1) * 1000)
            .setValue(1.0d)
            .build());
    expectLastCall();
    mockPointHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric4.test")
            .setHost("test3")
            .setTimestamp((alignedStartTimeEpochSeconds + 2) * 1000)
            .setValue(2.0d)
            .build());
    expectLastCall();
    mockTraceSpanLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("dummy")
            .setTraceId(traceId)
            .setSpanId("testspanid")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp1)
                        .setFields(ImmutableMap.of("key", "value", "key2", "value2"))
                        .build(),
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp2)
                        .setFields(ImmutableMap.of("key3", "value3", "key4", "value4"))
                        .build()))
            .build());
    expectLastCall();
    mockTraceSpanLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("dummy")
            .setTraceId(traceId)
            .setSpanId("testspanid")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp1)
                        .setFields(ImmutableMap.of("key", "value", "key2", "value2"))
                        .build(),
                    SpanLog.newBuilder()
                        .setTimestamp(timestamp2)
                        .setFields(ImmutableMap.of("key3", "value3"))
                        .build()))
            .setSpan(spanData)
            .build());
    expectLastCall();
    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(alignedStartTimeEpochSeconds * 1000)
            .setDuration(1000)
            .setName("testSpanName")
            .setSource("testsource")
            .setSpanId("testspanid")
            .setTraceId(traceId)
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("parent", "parent1"), new Annotation("parent", "parent2")))
            .build());
    expectLastCall();
    mockPointHandler.reject(anyString(), anyString());
    expectLastCall().times(2);

    replay(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler);

    String payloadStr =
        "metric4.test 0 "
            + alignedStartTimeEpochSeconds
            + " source=test1\n"
            + "metric4.test 1 "
            + (alignedStartTimeEpochSeconds + 1)
            + " source=test2\n"
            + "metric4.test 2 "
            + (alignedStartTimeEpochSeconds + 2)
            + " source=test3"; // note the lack of newline at the end!
    String histoData =
        "!M "
            + alignedStartTimeEpochSeconds
            + " #5 10.0 #10 100.0 metric.test.histo source=test1\n"
            + "!M "
            + (alignedStartTimeEpochSeconds + 60)
            + " #5 20.0 #6 30.0 #7 40.0 metric.test.histo source=test2";
    String spanLogData =
        "{\"spanId\":\"testspanid\",\"traceId\":\""
            + traceId
            + "\",\"logs\":[{\"timestamp\":"
            + timestamp1
            + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n";
    String spanLogDataWithSpanField =
        "{\"spanId\":\"testspanid\",\"traceId\":\""
            + traceId
            + "\",\"logs\":[{\"timestamp\":"
            + timestamp1
            + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ",\"fields\":{\"key3\":\"value3\"}}],"
            + "\"span\":\""
            + escapeSpanData(spanData)
            + "\"}\n";
    String badData =
        "@SourceTag action=save source=testSource newtag1 newtag2\n"
            + "@Event "
            + alignedStartTimeEpochSeconds
            + " \"Event name for testing\" host=host1 host=host2 tag=tag1 "
            + "severity=INFO multi=bar multi=baz\n"
            + "!M "
            + (alignedStartTimeEpochSeconds + 60)
            + " #5 20.0 #6 30.0 #7 40.0 metric.test.histo source=test2";

    assertEquals(
        500,
        gzippedHttpPost(
            "http://localhost:" + port + "/api/v2/wfproxy/checkin",
            "{}")); // apiContainer not available
    assertEquals(
        200,
        gzippedHttpPost(
            "http://localhost:" + port + "/api/v2/wfproxy/report?format=wavefront", payloadStr));
    assertEquals(
        200,
        gzippedHttpPost(
            "http://localhost:" + port + "/api/v2/wfproxy/report?format=histogram", histoData));
    assertEquals(
        200,
        gzippedHttpPost(
            "http://localhost:" + port + "/api/v2/wfproxy/report?format=trace", spanData));
    assertEquals(
        200,
        gzippedHttpPost(
            "http://localhost:" + port + "/api/v2/wfproxy/report?format=spanLogs", spanLogData));
    assertEquals(
        200,
        gzippedHttpPost(
            "http://localhost:" + port + "/api/v2/wfproxy/report?format=spanLogs",
            spanLogDataWithSpanField));
    entityPropertiesFactoryMap
        .get("central")
        .get(ReportableEntityType.HISTOGRAM)
        .setFeatureDisabled(true);
    assertEquals(
        403,
        gzippedHttpPost(
            "http://localhost:" + port + "/api/v2/wfproxy/report?format=histogram", histoData));

    entityPropertiesFactoryMap
        .get("central")
        .get(ReportableEntityType.TRACE)
        .setFeatureDisabled(true);
    assertEquals(
        403,
        gzippedHttpPost(
            "http://localhost:" + port + "/api/v2/wfproxy/report?format=trace", spanData));

    entityPropertiesFactoryMap
        .get("central")
        .get(ReportableEntityType.TRACE_SPAN_LOGS)
        .setFeatureDisabled(true);
    assertEquals(
        403,
        gzippedHttpPost(
            "http://localhost:" + port + "/api/v2/wfproxy/report?format=spanLogs", spanLogData));
    assertEquals(
        403,
        gzippedHttpPost(
            "http://localhost:" + port + "/api/v2/wfproxy/report?format=spanLogs",
            spanLogDataWithSpanField));
    assertEquals(
        400,
        gzippedHttpPost(
            "http://localhost:" + port + "/api/v2/wfproxy/report?format=wavefront", badData));
    verify(mockPointHandler, mockHistogramHandler, mockTraceHandler, mockTraceSpanLogsHandler);
  }

  @Test
  public void testHealthCheckAdminPorts() throws Exception {
    int port = findAvailablePort();
    int port2 = findAvailablePort();
    int port3 = findAvailablePort();
    int port4 = findAvailablePort();
    int adminPort = findAvailablePort();
    proxy.proxyConfig.pushListenerPorts = port + "," + port2 + "," + port3 + "," + port4;
    proxy.proxyConfig.adminApiListenerPort = adminPort;
    proxy.proxyConfig.httpHealthCheckPath = "/health";
    proxy.proxyConfig.httpHealthCheckAllPorts = true;
    proxy.proxyConfig.httpHealthCheckFailStatusCode = 403;
    proxy.healthCheckManager = new HealthCheckManagerImpl(proxy.proxyConfig);
    SpanSampler sampler = new SpanSampler(new RateSampler(1.0D), () -> null);
    proxy.startGraphiteListener(port, mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(port2, mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(port3, mockHandlerFactory, null, sampler);
    proxy.startGraphiteListener(port4, mockHandlerFactory, null, sampler);
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
    int port = findAvailablePort();
    proxy.proxyConfig.pushListenerPorts = String.valueOf(port);
    proxy.startGraphiteListener(
        port, mockHandlerFactory, null, new SpanSampler(new RateSampler(1.0D), () -> null));
    waitUntilListenerIsOnline(port);
    reset(mockHistogramHandler);
    List<Double> bins = new ArrayList<>();
    List<Integer> counts = new ArrayList<>();
    for (int i = 0; i < 50; i++) bins.add(10.0d);
    for (int i = 0; i < 150; i++) bins.add(99.0d);
    for (int i = 0; i < 200; i++) counts.add(1);
    mockHistogramHandler.report(
        ReportPoint.newBuilder()
            .setTable("dummy")
            .setMetric("metric.test.histo")
            .setHost("test1")
            .setTimestamp(alignedStartTimeEpochSeconds * 1000)
            .setValue(
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
    payloadStr.append(alignedStartTimeEpochSeconds);
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

  @Test
  public void testIgnoreBackendSpanHeadSamplingPercent() {
    proxy.proxyConfig.backendSpanHeadSamplingPercentIgnored = true;
    proxy.proxyConfig.traceSamplingRate = 1.0;
    AgentConfiguration agentConfiguration = new AgentConfiguration();
    agentConfiguration.setSpanSamplingRate(0.5);
    proxy.processConfiguration("cetnral", agentConfiguration);
    assertEquals(
        1.0,
        entityPropertiesFactoryMap.get("central").getGlobalProperties().getTraceSamplingRate(),
        1e-3);

    proxy.proxyConfig.backendSpanHeadSamplingPercentIgnored = false;
    proxy.processConfiguration("central", agentConfiguration);
    assertEquals(
        0.5,
        entityPropertiesFactoryMap.get("central").getGlobalProperties().getTraceSamplingRate(),
        1e-3);
  }
}
