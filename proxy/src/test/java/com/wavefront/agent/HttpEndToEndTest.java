package com.wavefront.agent;

import static com.wavefront.agent.ProxyUtil.createInitializer;
import static com.wavefront.agent.TestUtils.*;
import static com.wavefront.agent.channel.ChannelUtils.makeResponse;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static com.wavefront.api.agent.Constants.PUSH_FORMAT_LOGS_JSON_ARR;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.listeners.AbstractHttpOnlyHandler;
import com.wavefront.common.Clock;
import com.wavefront.ingester.TcpIngester;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HttpEndToEndTest {
  private static final Logger logger = Logger.getLogger("test");

  public static int HTTP_timeout_tests = 1000;

  private static PushAgent proxy;
  private static MutableFunc<FullHttpRequest, HttpResponse> server = new MutableFunc<>(x -> null);
  private static Thread thread;
  private static int backendPort;

  private static int pushPort;
  private static AtomicLong digestTime;
  private static int histMinPort;
  private static int histHourPort;
  private static int histDayPort;
  private static int histDistPort;
  private static int tracesPort;
  private static int deltaAggregationPort;

  @BeforeClass
  public static void setup() throws Exception {
    backendPort = findAvailablePort(8081);
    ChannelHandler channelHandler = new WrappingHttpHandler(null, null, backendPort, server);
    thread =
        new Thread(
            new TcpIngester(
                createInitializer(
                    channelHandler, backendPort, 32768, 16 * 1024 * 1024, 5, null, null),
                backendPort));
    thread.start();
    waitUntilListenerIsOnline(backendPort);

    digestTime = new AtomicLong(System.currentTimeMillis());

    pushPort = findAvailablePort(2898);
    tracesPort = findAvailablePort(3000);
    histMinPort = findAvailablePort(40001);
    histHourPort = findAvailablePort(40002);
    histDayPort = findAvailablePort(40003);
    histDistPort = findAvailablePort(40000);
    deltaAggregationPort = findAvailablePort(50000);

    proxy = new PushAgent();
    proxy.proxyConfig.server = "http://localhost:" + backendPort + "/api/";
    proxy.proxyConfig.flushThreads = 1;
    proxy.proxyConfig.pushListenerPorts = String.valueOf(pushPort);
    proxy.proxyConfig.pushFlushInterval = 50;
    proxy.proxyConfig.gzipCompression = false;
    proxy.proxyConfig.pushFlushMaxPoints = 1;
    proxy.proxyConfig.disableBuffer = true;

    proxy.proxyConfig.flushThreadsSourceTags = 1;
    proxy.proxyConfig.splitPushWhenRateLimited = true;
    proxy.proxyConfig.pushRateLimitSourceTags = 100;

    proxy.proxyConfig.histogramMinuteListenerPorts = String.valueOf(histMinPort);
    proxy.proxyConfig.histogramHourListenerPorts = String.valueOf(histHourPort);
    proxy.proxyConfig.histogramDayListenerPorts = String.valueOf(histDayPort);
    proxy.proxyConfig.histogramDistListenerPorts = String.valueOf(histDistPort);
    proxy.proxyConfig.histogramMinuteAccumulatorPersisted = false;
    proxy.proxyConfig.histogramHourAccumulatorPersisted = false;
    proxy.proxyConfig.histogramDayAccumulatorPersisted = false;
    proxy.proxyConfig.histogramDistAccumulatorPersisted = false;
    proxy.proxyConfig.histogramMinuteMemoryCache = false;
    proxy.proxyConfig.histogramHourMemoryCache = false;
    proxy.proxyConfig.histogramDayMemoryCache = false;
    proxy.proxyConfig.histogramDistMemoryCache = false;
    proxy.proxyConfig.histogramMinuteFlushSecs = 1;
    proxy.proxyConfig.histogramHourFlushSecs = 1;
    proxy.proxyConfig.histogramDayFlushSecs = 1;
    proxy.proxyConfig.histogramDistFlushSecs = 1;
    proxy.proxyConfig.histogramMinuteAccumulatorSize = 10L;
    proxy.proxyConfig.histogramHourAccumulatorSize = 10L;
    proxy.proxyConfig.histogramDayAccumulatorSize = 10L;
    proxy.proxyConfig.histogramDistAccumulatorSize = 10L;
    proxy.proxyConfig.histogramAccumulatorFlushInterval = 10000L;
    proxy.proxyConfig.histogramAccumulatorResolveInterval = 10000L;
    proxy.proxyConfig.timeProvider = digestTime::get;

    proxy.proxyConfig.traceListenerPorts = String.valueOf(tracesPort);
    proxy.proxyConfig.deltaCountersAggregationIntervalSeconds = 2;

    proxy.proxyConfig.deltaCountersAggregationListenerPorts = String.valueOf(deltaAggregationPort);

    proxy.start(new String[] {});
  }

  @AfterClass
  public static void teardown() {
    thread.interrupt();
    proxy.shutdown();
  }

  @Test
  public void testEndToEndDelta() throws Exception {
    waitUntilListenerIsOnline(deltaAggregationPort);
    String payloadStr1 = "∆test.mixed1 1.0 source=test1\n";
    String payloadStr2 = "∆test.mixed2 2.0 source=test1\n";
    String payloadStr3 = "test.mixed3 3.0 source=test1\n";
    String payloadStr4 = "∆test.mixed3 3.0 source=test1\n";

    AtomicBoolean ok = new AtomicBoolean(false);
    server.update(
        req -> {
          String content = req.content().toString(CharsetUtil.UTF_8);
          logger.fine("Content received: " + content);
          List<String> points = Arrays.asList(content.split("\n"));
          points.stream()
              .filter(s -> s.length() > 0)
              .forEach(s -> assertTrue(s.trim().matches("(.*)test.mixed[123]\" [143].0(.*)")));
          ok.set(true);
          return makeResponse(HttpResponseStatus.OK, "");
        });
    gzippedHttpPost(
        "http://localhost:" + deltaAggregationPort + "/",
        payloadStr1 + payloadStr2 + payloadStr2 + payloadStr3 + payloadStr4);
    assertTrueWithTimeout(HTTP_timeout_tests * 10, ok::get);
  }

  @Test
  public void testEndToEndMetrics() throws Exception {

    long time = Clock.now() / 1000;
    waitUntilListenerIsOnline(pushPort);

    String payload =
        "metric.name 1 "
            + time
            + " source=metric.source tagk1=tagv1\n"
            + "metric.name 2 "
            + time
            + " source=metric.source tagk1=tagv2\n"
            + "metric.name 3 "
            + time
            + " source=metric.source tagk1=tagv3\n"
            + "metric.name 4 "
            + time
            + " source=metric.source tagk1=tagv4\n";
    String expectedTest1part1 =
        "\"metric.name\" 1.0 "
            + time
            + " source=\"metric.source\" \"tagk1\"=\"tagv1\"\n"
            + "\"metric.name\" 2.0 "
            + time
            + " source=\"metric.source\" \"tagk1\"=\"tagv2\"";
    String expectedTest1part2 =
        "\"metric.name\" 3.0 "
            + time
            + " source=\"metric.source\" \"tagk1\"=\"tagv3\"\n"
            + "\"metric.name\" 4.0 "
            + time
            + " source=\"metric.source\" \"tagk1\"=\"tagv4\"";

    AtomicBoolean ok = new AtomicBoolean(false);
    server.update(
        req -> {
          String content = req.content().toString(CharsetUtil.UTF_8);
          logger.fine("Content received: " + content);
          assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
          ok.set(true);
          return makeResponse(HttpResponseStatus.OK, "");
        });
    gzippedHttpPost("http://localhost:" + pushPort + "/", payload);
    assertTrueWithTimeout(HTTP_timeout_tests * 20, ok::get);

    AtomicInteger successfulSteps = new AtomicInteger(0);
    AtomicInteger testCounter = new AtomicInteger(0);
    AtomicBoolean OK = new AtomicBoolean(false);
    server.update(
        req -> {
          String content = req.content().toString(CharsetUtil.UTF_8);
          logger.info("testCounter=" + testCounter.incrementAndGet());
          logger.fine("Content received: " + content);
          switch (testCounter.get()) {
            case 1:
              assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
              successfulSteps.incrementAndGet();
              return makeResponse(HttpResponseStatus.TOO_MANY_REQUESTS, "");
              // case 2: // TODO: review
              // assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
              // successfulSteps.incrementAndGet();
              // return makeResponse(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, "");
            case 10:
              assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
              successfulSteps.incrementAndGet();
              OK.set(true);
              return makeResponse(HttpResponseStatus.OK, "");
            default:
              assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
              successfulSteps.incrementAndGet();
              return makeResponse(HttpResponseStatus.valueOf(407), "");
          }
        });
    gzippedHttpPost("http://localhost:" + pushPort + "/", payload);
    assertTrueWithTimeout(HTTP_timeout_tests * 10, OK::get);
  }

  @Test
  public void testEndToEndEvents() throws Exception {
    long time = Clock.now() / 1000;
    String payload_1 =
        "@Event "
            + time
            + " \"Event name for testing\" host=host1 host=host2 tag=tag1 "
            + "severity=INFO multi=bar multi=baz\n";
    String expected_1 =
        "{\"name\": \"Event name for testing\", \"startTime\": "
            + (time * 1000)
            + ", \"endTime\": "
            + (time * 1000 + 1)
            + ", \"annotations\": {\"severity\": \"INFO\"}, "
            + "\"hosts\": [\"host1\", \"host2\"], "
            + "\"tags\": [\"tag1\"], "
            + "\"dimensions\": {\"multi\": [\"bar\", \"baz\"]}}";

    String payload_2 = "@Event " + time + " \"Another test event\" host=host3";
    String expected_2 =
        "{\"name\": \"Another test event\", \"startTime\": "
            + (time * 1000)
            + ", \"endTime\": "
            + (time * 1000 + 1)
            + ", \"annotations\": {}, "
            + "\"hosts\": [\"host3\"], "
            + "\"tags\": null, "
            + "\"dimensions\": null}";
    testEndToEndEvents(payload_1, expected_1);
    testEndToEndEvents(payload_2, expected_2);
  }

  public void testEndToEndEvents(String payload, String expected) throws Exception {
    AtomicInteger successfulSteps = new AtomicInteger(0);
    AtomicInteger testCounter = new AtomicInteger(0);

    waitUntilListenerIsOnline(pushPort);

    AtomicBoolean ok = new AtomicBoolean(false);
    server.update(
        req -> {
          String content = req.content().toString(CharsetUtil.UTF_8);
          URI uri;
          try {
            uri = new URI(req.uri());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          String path = uri.getPath();
          logger.fine("Content received: " + content);
          assertEquals(HttpMethod.POST, req.method());
          assertEquals("/api/v2/wfproxy/event", path);
          System.out.println("testCounter: " + testCounter.incrementAndGet());
          System.out.println("-> " + content);
          assertThat(content, containsString(expected));
          switch (testCounter.get()) {
              // TODO: review/implement
              // return makeResponse(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, "");
            default:
              successfulSteps.incrementAndGet();
              return makeResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, "");
            case 10:
              ok.set(true);
              return makeResponse(HttpResponseStatus.OK, "");
          }
        });
    gzippedHttpPost("http://localhost:" + pushPort + "/", payload);
    assertTrueWithTimeout(HTTP_timeout_tests * 10, ok::get);
  }

  @Test
  public void testEndToEndSourceTags() throws Exception {
    waitUntilListenerIsOnline(pushPort);

    String payloadSourceTags =
        "@SourceTag action=add source=testSource addTag1 addTag2 addTag3\n"
            + "@SourceTag action=save source=testSource newtag1 newtag2\n"
            + "@SourceTag action=delete source=testSource deleteTag\n"
            + "@SourceDescription action=save source=testSource \"Long Description\"\n"
            + "@SourceDescription action=delete source=testSource";

    String[][] expected = {
      {"/api/v2/source/testSource/tag/addTag1", ""},
      {"/api/v2/source/testSource/tag/addTag2", ""},
      {"/api/v2/source/testSource/tag/addTag3", ""},
      {"/api/v2/source/testSource/tag", "[\"newtag1\",\"newtag2\"]"},
      {"/api/v2/source/testSource/tag/deleteTag", ""},
      {"/api/v2/source/testSource/description", "Long Description"},
      {"/api/v2/source/testSource/description", ""}
    };
    List<String[]> urlsCalled = new ArrayList<>();
    server.update(
        req -> {
          String content = req.content().toString(CharsetUtil.UTF_8);
          System.out.println("-=>" + content);
          URI uri;
          try {
            uri = new URI(req.uri());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          String path = uri.getPath();
          urlsCalled.add(new String[] {path, content});
          return makeResponse(HttpResponseStatus.OK, "");
        });
    gzippedHttpPost("http://localhost:" + pushPort + "/", payloadSourceTags);
    assertTrueWithTimeout(HTTP_timeout_tests * 10, () -> 7 == urlsCalled.size());
    assertArrayEquals(expected, urlsCalled.toArray());
  }

  @Test
  public void testEndToEndHistograms() throws Exception {
    AtomicInteger successfulSteps = new AtomicInteger(0);
    AtomicInteger testCounter = new AtomicInteger(0);
    long time = (Clock.now() / 1000) / 60 * 60 + 30;

    waitUntilListenerIsOnline(histDistPort);

    String payloadHistograms =
        "metric.name 1 "
            + time
            + " source=metric.source tagk1=tagv1\n"
            + "metric.name 1 "
            + time
            + " source=metric.source tagk1=tagv1\n"
            + "metric.name 2 "
            + (time + 1)
            + " source=metric.source tagk1=tagv1\n"
            + "metric.name 2 "
            + (time + 2)
            + " source=metric.source tagk1=tagv1\n"
            + "metric.name 3 "
            + time
            + " source=metric.source tagk1=tagv2\n"
            + "metric.name 4 "
            + time
            + " source=metric.source tagk1=tagv2\n"
            + "metric.name 5 "
            + (time + 60)
            + " source=metric.source tagk1=tagv1\n"
            + "metric.name 6 "
            + (time + 60)
            + " source=metric.source tagk1=tagv1\n";

    long minuteBin = time / 60 * 60;
    long hourBin = time / 3600 * 3600;
    long dayBin = time / 86400 * 86400;
    Set<String> expectedHistograms =
        ImmutableSet.of(
            "!M "
                + minuteBin
                + " #2 1.0 #2 2.0 \"metric.name\" "
                + "source=\"metric.source\" \"tagk1\"=\"tagv1\"",
            "!M "
                + minuteBin
                + " #1 3.0 #1 4.0 \"metric.name\" source=\"metric.source\" "
                + "\"tagk1\"=\"tagv2\"",
            "!M "
                + (minuteBin + 60)
                + " #1 5.0 #1 6.0 \"metric.name\" source=\"metric.source\" "
                + "\"tagk1\"=\"tagv1\"",
            "!H "
                + hourBin
                + " #2 1.0 #2 2.0 #1 5.0 #1 6.0 \"metric.name\" "
                + "source=\"metric.source\" \"tagk1\"=\"tagv1\"",
            "!H "
                + hourBin
                + " #1 3.0 #1 4.0 \"metric.name\" source=\"metric.source\" "
                + "\"tagk1\"=\"tagv2\"",
            "!D "
                + dayBin
                + " #1 3.0 #1 4.0 \"metric.name\" source=\"metric.source\" "
                + "\"tagk1\"=\"tagv2\"",
            "!D "
                + dayBin
                + " #2 1.0 #2 2.0 #1 5.0 #1 6.0 \"metric.name\" "
                + "source=\"metric.source\" \"tagk1\"=\"tagv1\"");

    String distPayload =
        "!M "
            + minuteBin
            + " #1 1.0 #1 1.0 #1 2.0 #1 2.0 \"metric.name\" "
            + "source=\"metric.source\" \"tagk1\"=\"tagv1\"\n"
            + "!M "
            + minuteBin
            + " #1 1.0 #1 1.0 #1 2.0 #1 2.0 \"metric.name\" "
            + "source=\"metric.source\" \"tagk1\"=\"tagv1\"\n"
            + "!H "
            + minuteBin
            + " #1 1.0 #1 1.0 #1 2.0 #1 2.0 \"metric.name\" "
            + "source=\"metric.source\" \"tagk1\"=\"tagv1\"\n";

    Set<String> expectedDists =
        ImmutableSet.of(
            "!M "
                + minuteBin
                + " #4 1.0 #4 2.0 "
                + "\"metric.name\" source=\"metric.source\" \"tagk1\"=\"tagv1\"",
            "!H "
                + hourBin
                + " #2 1.0 #2 2.0 \"metric.name\" "
                + "source=\"metric.source\" \"tagk1\"=\"tagv1\"");

    server.update(
        req -> {
          String content = req.content().toString(CharsetUtil.UTF_8);
          URI uri;
          try {
            uri = new URI(req.uri());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          String path = uri.getPath();
          assertEquals(HttpMethod.POST, req.method());
          assertEquals("/api/v2/wfproxy/report", path);
          logger.info("Content received: " + content);
          switch (testCounter.incrementAndGet()) {
            case 1:
              HashSet<String> histograms = new HashSet<>(Arrays.asList(content.split("\n")));
              assertEquals(expectedHistograms, histograms);
              successfulSteps.incrementAndGet();
              return makeResponse(HttpResponseStatus.OK, "");
            case 2:
              assertEquals(expectedDists, new HashSet<>(Arrays.asList(content.split("\n"))));
              successfulSteps.incrementAndGet();
              return makeResponse(HttpResponseStatus.OK, "");
          }
          return makeResponse(HttpResponseStatus.OK, "");
        });
    digestTime.set(System.currentTimeMillis() - 1001);
    gzippedHttpPost("http://localhost:" + histMinPort + "/", payloadHistograms);
    gzippedHttpPost("http://localhost:" + histHourPort + "/", payloadHistograms);
    gzippedHttpPost("http://localhost:" + histDayPort + "/", payloadHistograms);
    gzippedHttpPost("http://localhost:" + histDistPort + "/", payloadHistograms); // should reject
    digestTime.set(System.currentTimeMillis());
    assertTrueWithTimeout(HTTP_timeout_tests * 10, () -> 1 == successfulSteps.get());

    digestTime.set(System.currentTimeMillis() - 1001);
    gzippedHttpPost("http://localhost:" + histDistPort + "/", distPayload);
    digestTime.set(System.currentTimeMillis());
    proxy.histogramFlushRunnables.forEach(Runnable::run);
    assertTrueWithTimeout(HTTP_timeout_tests * 10, () -> 2 == successfulSteps.get());
  }

  @Test
  public void testEndToEndSpans() throws Exception {
    long time = Clock.now() / 1000;
    waitUntilListenerIsOnline(tracesPort);

    String traceId = UUID.randomUUID().toString();
    long timestamp1 = time * 1000000 + 12345;
    long timestamp2 = time * 1000000 + 23456;
    String payload =
        "testSpanName parent=parent1 source=testsource spanId=testspanid "
            + "traceId=\""
            + traceId
            + "\" parent=parent2 "
            + time
            + " "
            + (time + 1)
            + "\n"
            + "{\"spanId\":\"testspanid\",\"traceId\":\""
            + traceId
            + "\",\"logs\":[{\"timestamp\":"
            + timestamp1
            + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n";
    String expectedSpan =
        "\"testSpanName\" source=\"testsource\" spanId=\"testspanid\" "
            + "traceId=\""
            + traceId
            + "\" \"parent\"=\"parent1\" \"parent\"=\"parent2\" "
            + (time * 1000)
            + " 1000";
    String expectedSpanLog =
        "{\"customer\":\"dummy\",\"traceId\":\""
            + traceId
            + "\",\"spanId"
            + "\":\"testspanid\",\"spanSecondaryId\":null,\"logs\":[{\"timestamp\":"
            + timestamp1
            + ","
            + "\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ","
            + "\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}],\"span\":\"_sampledByPolicy=NONE\"}";
    AtomicBoolean gotSpan = new AtomicBoolean(false);
    AtomicBoolean gotSpanLog = new AtomicBoolean(false);
    server.update(
        req -> {
          String content = req.content().toString(CharsetUtil.UTF_8);
          logger.fine("Content received: " + content);
          if (content.equals(expectedSpan)) gotSpan.set(true);
          if (content.equals(expectedSpanLog)) gotSpanLog.set(true);
          return makeResponse(HttpResponseStatus.OK, "");
        });
    gzippedHttpPost("http://localhost:" + tracesPort + "/", payload);
    assertTrueWithTimeout(HTTP_timeout_tests, gotSpan::get);
    assertTrueWithTimeout(HTTP_timeout_tests, gotSpanLog::get);
  }

  @Test
  public void testEndToEndSpans_SpanLogsWithSpanField() throws Exception {
    long time = Clock.now() / 1000;
    waitUntilListenerIsOnline(tracesPort);

    String traceId = UUID.randomUUID().toString();
    long timestamp1 = time * 1000000 + 12345;
    long timestamp2 = time * 1000000 + 23456;
    String payload =
        "testSpanName parent=parent1 source=testsource spanId=testspanid "
            + "traceId=\""
            + traceId
            + "\" parent=parent2 "
            + time
            + " "
            + (time + 1)
            + "\n"
            + "{\"spanId\":\"testspanid\",\"traceId\":\""
            + traceId
            + "\",\"logs\":[{\"timestamp\":"
            + timestamp1
            + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}],\"span\":\""
            + "testSpanName parent=parent1 source=testsource spanId=testspanid traceId=\\\""
            + traceId
            + "\\\" parent=parent2 "
            + time
            + " "
            + (time + 1)
            + "\\n\"}\n";
    String expectedSpan =
        "\"testSpanName\" source=\"testsource\" spanId=\"testspanid\" "
            + "traceId=\""
            + traceId
            + "\" \"parent\"=\"parent1\" \"parent\"=\"parent2\" "
            + (time * 1000)
            + " 1000";
    String expectedSpanLog =
        "{\"customer\":\"dummy\",\"traceId\":\""
            + traceId
            + "\",\"spanId"
            + "\":\"testspanid\",\"spanSecondaryId\":null,\"logs\":[{\"timestamp\":"
            + timestamp1
            + ","
            + "\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":"
            + timestamp2
            + ","
            + "\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}],\"span\":\"_sampledByPolicy=NONE\"}";
    AtomicBoolean gotSpan = new AtomicBoolean(false);
    AtomicBoolean gotSpanLog = new AtomicBoolean(false);
    server.update(
        req -> {
          String content = req.content().toString(CharsetUtil.UTF_8);
          logger.fine("Content received: " + content);
          if (content.equals(expectedSpan)) gotSpan.set(true);
          if (content.equals(expectedSpanLog)) gotSpanLog.set(true);
          return makeResponse(HttpResponseStatus.OK, "");
        });
    gzippedHttpPost("http://localhost:" + tracesPort + "/", payload);
    assertTrueWithTimeout(HTTP_timeout_tests * 10, gotSpan::get);
    assertTrueWithTimeout(HTTP_timeout_tests, gotSpanLog::get);
  }

  @Test
  public void testEndToEndLogs() throws Exception {
    long time = Clock.now() / 1000;
    waitUntilListenerIsOnline(pushPort);

    long timestamp = time * 1000 + 12345;
    String payload =
        "[{\"source\": \"myHost\",\n \"timestamp\": \""
            + timestamp
            + "\", "
            + "\"application\":\"myApp\",\"service\":\"myService\","
            + "\"log_level\":\"WARN\",\"error_name\":\"myException\""
            + "}]";
    String expectedLog =
        "[{\"timestamp\":"
            + timestamp
            + ",\"text\":\"\",\"application\":\"myApp\",\"service\":\"myService\","
            + "\"log_level\":\"WARN\",\"error_name\":\"myException\""
            + "}]";
    AtomicBoolean gotLog = new AtomicBoolean(false);
    server.update(
        req -> {
          String content = req.content().toString(CharsetUtil.UTF_8);
          logger.fine("Content received: " + content);
          if (content.equals(expectedLog)) gotLog.set(true);
          return makeResponse(HttpResponseStatus.OK, "");
        });
    gzippedHttpPost("http://localhost:" + pushPort + "/?f=" + PUSH_FORMAT_LOGS_JSON_ARR, payload);

    assertTrueWithTimeout(HTTP_timeout_tests * 10, gotLog::get);
  }

  private static class WrappingHttpHandler extends AbstractHttpOnlyHandler {
    private final Function<FullHttpRequest, HttpResponse> func;

    public WrappingHttpHandler(
        @Nullable TokenAuthenticator tokenAuthenticator,
        @Nullable HealthCheckManager healthCheckManager,
        @Nullable int port,
        @Nonnull Function<FullHttpRequest, HttpResponse> func) {
      super(tokenAuthenticator, healthCheckManager, port);
      this.func = func;
    }

    @Override
    protected void handleHttpMessage(ChannelHandlerContext ctx, FullHttpRequest request) {
      URI uri;
      try {
        uri = new URI(request.uri());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      String path = uri.getPath();
      logger.fine("Incoming HTTP request: " + uri.getPath());
      if (path.endsWith("/checkin")
          && (path.startsWith("/api/daemon") || path.contains("wfproxy"))) {
        // simulate checkin response for proxy chaining
        ObjectNode jsonResponse = JsonNodeFactory.instance.objectNode();
        jsonResponse.put("currentTime", Clock.now());
        jsonResponse.put("allowAnyHostKeys", true);
        jsonResponse.put("logServerEndpointUrl", "http://localhost:" + port + "/api/");
        jsonResponse.put("logServerToken", "12345");
        writeHttpResponse(ctx, HttpResponseStatus.OK, jsonResponse, request);
        return;
      } else if (path.endsWith("/config/processed")) {
        writeHttpResponse(ctx, HttpResponseStatus.OK, "", request);
        return;
      }
      HttpResponse response = func.apply(request);
      logger.fine("Responding with HTTP " + response.status());
      writeHttpResponse(ctx, response, request);
    }
  }

  private static class MutableFunc<T, R> implements Function<T, R> {
    private Function<T, R> delegate;

    public MutableFunc(Function<T, R> delegate) {
      this.delegate = delegate;
    }

    public void update(Function<T, R> delegate) {
      this.delegate = delegate;
    }

    @Override
    public R apply(T t) {
      return delegate.apply(t);
    }
  }
}
