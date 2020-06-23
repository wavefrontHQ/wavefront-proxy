package com.wavefront.agent;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.SenderTaskFactoryImpl;
import com.wavefront.agent.listeners.AbstractHttpOnlyHandler;
import com.wavefront.agent.queueing.QueueingFactoryImpl;
import com.wavefront.common.Clock;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.TcpIngester;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Logger;

import static com.wavefront.agent.ProxyUtil.createInitializer;
import static com.wavefront.agent.TestUtils.assertTrueWithTimeout;
import static com.wavefront.agent.TestUtils.findAvailablePort;
import static com.wavefront.agent.TestUtils.gzippedHttpPost;
import static com.wavefront.agent.TestUtils.waitUntilListenerIsOnline;
import static com.wavefront.agent.channel.ChannelUtils.makeResponse;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author vasily@wavefront.com
 */
public class HttpEndToEndTest {
  private static final Logger logger = Logger.getLogger("test");

  private PushAgent proxy;
  private MutableFunc<FullHttpRequest, HttpResponse> server = new MutableFunc<>(x -> null);
  private Thread thread;
  private int backendPort;
  private int proxyPort;

  @Before
  public void setup() throws Exception {
    backendPort = findAvailablePort(8081);
    ChannelHandler channelHandler = new WrappingHttpHandler(null, null,
        String.valueOf(backendPort), server);
    thread = new Thread(new TcpIngester(createInitializer(channelHandler,
        backendPort, 32768, 16 * 1024 * 1024, 5, Optional.empty()), backendPort));
    thread.start();
    waitUntilListenerIsOnline(backendPort);
  }

  @After
  public void teardown() {
    thread.interrupt();
    proxy.stopListener(proxyPort);
    proxy.shutdown();
  }

  @Test
  public void testEndToEndMetrics() throws Exception {
    AtomicInteger successfulSteps = new AtomicInteger(0);
    AtomicInteger testCounter = new AtomicInteger(0);
    long time = Clock.now() / 1000;
    proxyPort = findAvailablePort(2898);
    String buffer = File.createTempFile("proxyTestBuffer", null).getPath();
    proxy = new PushAgent();
    proxy.proxyConfig.server = "http://localhost:" + backendPort + "/api/";
    proxy.proxyConfig.flushThreads = 1;
    proxy.proxyConfig.pushListenerPorts = String.valueOf(proxyPort);
    proxy.proxyConfig.pushFlushInterval = 50;
    proxy.proxyConfig.bufferFile = buffer;
    proxy.proxyConfig.allowRegex = "^.*$";
    proxy.proxyConfig.blockRegex = "^.*blocklist.*$";
    proxy.proxyConfig.gzipCompression = false;
    proxy.start(new String[]{});
    waitUntilListenerIsOnline(proxyPort);
    if (!(proxy.senderTaskFactory instanceof SenderTaskFactoryImpl)) fail();
    if (!(proxy.queueingFactory instanceof QueueingFactoryImpl)) fail();

    String payload =
        "metric.name 1 " + time + " source=metric.source tagk1=tagv1\n" +
        "metric.name 2 " + time + " source=metric.source tagk1=tagv2\n" +
        "metric.name 3 " + time + " source=metric.source tagk1=tagv3\n" +
        "metric.name 4 " + time + " source=metric.source tagk1=tagv4\n";
    String expectedTest1part1 =
        "\"metric.name\" 1.0 " + time + " source=\"metric.source\" \"tagk1\"=\"tagv1\"\n" +
            "\"metric.name\" 2.0 " + time + " source=\"metric.source\" \"tagk1\"=\"tagv2\"";
    String expectedTest1part2 =
        "\"metric.name\" 3.0 " + time + " source=\"metric.source\" \"tagk1\"=\"tagv3\"\n" +
            "\"metric.name\" 4.0 " + time + " source=\"metric.source\" \"tagk1\"=\"tagv4\"";

    server.update(req -> {
      String content = req.content().toString(CharsetUtil.UTF_8);
      logger.fine("Content received: " + content);
      assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
      successfulSteps.incrementAndGet();
      return makeResponse(HttpResponseStatus.OK, "");
    });
    gzippedHttpPost("http://localhost:" + proxyPort + "/", payload);
    HandlerKey key = HandlerKey.of(ReportableEntityType.POINT, String.valueOf(proxyPort));
    ((SenderTaskFactoryImpl) proxy.senderTaskFactory).flushNow(key);
    assertEquals(1, successfulSteps.getAndSet(0));
    AtomicBoolean part1 = new AtomicBoolean(false);
    AtomicBoolean part2 = new AtomicBoolean(false);
    server.update(req -> {
      String content = req.content().toString(CharsetUtil.UTF_8);
      logger.fine("Content received: " + content);
      switch (testCounter.incrementAndGet()) {
        case 1:
          assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.TOO_MANY_REQUESTS, "");
        case 2:
          assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
        case 3:
          assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.valueOf(407), "");
        case 4:
          assertEquals(expectedTest1part1 + "\n" + expectedTest1part2, content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, "");
        case 5:
        case 6:
          if (content.equals(expectedTest1part1)) part1.set(true);
          if (content.equals(expectedTest1part2)) part2.set(true);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
      }
      throw new IllegalStateException();
    });
    gzippedHttpPost("http://localhost:" + proxyPort + "/", payload);
    ((SenderTaskFactoryImpl) proxy.senderTaskFactory).flushNow(key);
    ((QueueingFactoryImpl) proxy.queueingFactory).flushNow(key);
    gzippedHttpPost("http://localhost:" + proxyPort + "/", payload);
    ((SenderTaskFactoryImpl) proxy.senderTaskFactory).flushNow(key);
    for (int i = 0; i < 3; i++) ((QueueingFactoryImpl) proxy.queueingFactory).flushNow(key);
    assertEquals(6, successfulSteps.getAndSet(0));
    assertTrue(part1.get());
    assertTrue(part2.get());
  }

  @Test
  public void testEndToEndEvents() throws Exception {
    AtomicInteger successfulSteps = new AtomicInteger(0);
    AtomicInteger testCounter = new AtomicInteger(0);
    long time = Clock.now() / 1000;
    proxyPort = findAvailablePort(2898);
    String buffer = File.createTempFile("proxyTestBuffer", null).getPath();
    proxy = new PushAgent();
    proxy.proxyConfig.server = "http://localhost:" + backendPort + "/api/";
    proxy.proxyConfig.flushThreads = 1;
    proxy.proxyConfig.flushThreadsEvents = 1;
    proxy.proxyConfig.pushListenerPorts = String.valueOf(proxyPort);
    proxy.proxyConfig.pushFlushInterval = 10000;
    proxy.proxyConfig.pushRateLimitEvents = 100;
    proxy.proxyConfig.bufferFile = buffer;
    proxy.start(new String[]{});
    waitUntilListenerIsOnline(proxyPort);
    if (!(proxy.senderTaskFactory instanceof SenderTaskFactoryImpl)) fail();
    if (!(proxy.queueingFactory instanceof QueueingFactoryImpl)) fail();

    String payloadEvents =
        "@Event " + time + " \"Event name for testing\" host=host1 host=host2 tag=tag1 " +
            "severity=INFO multi=bar multi=baz\n" +
            "@Event " + time + " \"Another test event\" host=host3";
    String expectedEvent1 = "{\"name\":\"Event name for testing\",\"startTime\":" + (time * 1000) +
        ",\"endTime\":" + (time * 1000 + 1) + ",\"annotations\":{\"severity\":\"INFO\"}," +
        "\"dimensions\":{\"multi\":[\"bar\",\"baz\"]},\"hosts\":[\"host1\",\"host2\"]," +
        "\"tags\":[\"tag1\"]}";
    String expectedEvent2 = "{\"name\":\"Another test event\",\"startTime\":" + (time * 1000) +
        ",\"endTime\":" + (time * 1000 + 1) + ",\"annotations\":{},\"dimensions\":null," +
        "\"hosts\":[\"host3\"],\"tags\":null}";
    server.update(req -> {
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
      switch (testCounter.incrementAndGet()) {
        case 1:
          assertEquals("[" + expectedEvent1 + "," + expectedEvent2 + "]", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, "");
        case 2:
          assertEquals("[" + expectedEvent1 + "]", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
        case 3:
          assertEquals("[" + expectedEvent2 + "]", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
        case 4:
          assertEquals("[" + expectedEvent1 + "," + expectedEvent2 + "]", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.valueOf(407), "");
        case 5:
          assertEquals("[" + expectedEvent1 + "," + expectedEvent2 + "]", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, "");
        case 6:
          assertEquals("[" + expectedEvent1 + "," + expectedEvent2 + "]", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
      }
      logger.warning("Too many requests");
      successfulSteps.incrementAndGet(); // this will force the assert to fail
      return makeResponse(HttpResponseStatus.OK, "");
    });
    gzippedHttpPost("http://localhost:" + proxyPort + "/", payloadEvents);
    HandlerKey key = HandlerKey.of(ReportableEntityType.EVENT, String.valueOf(proxyPort));
    ((SenderTaskFactoryImpl) proxy.senderTaskFactory).flushNow(key);
    ((QueueingFactoryImpl) proxy.queueingFactory).flushNow(key);
    gzippedHttpPost("http://localhost:" + proxyPort + "/", payloadEvents);
    ((SenderTaskFactoryImpl) proxy.senderTaskFactory).flushNow(key);
    for (int i = 0; i < 2; i++) ((QueueingFactoryImpl) proxy.queueingFactory).flushNow(key);
    assertEquals(6, successfulSteps.getAndSet(0));
  }

  @Test
  public void testEndToEndSourceTags() throws Exception {
    AtomicInteger successfulSteps = new AtomicInteger(0);
    AtomicInteger testCounter = new AtomicInteger(0);
    proxyPort = findAvailablePort(2898);
    String buffer = File.createTempFile("proxyTestBuffer", null).getPath();
    proxy = new PushAgent();
    proxy.proxyConfig.server = "http://localhost:" + backendPort + "/api/";
    proxy.proxyConfig.flushThreads = 1;
    proxy.proxyConfig.flushThreadsSourceTags = 1;
    proxy.proxyConfig.splitPushWhenRateLimited = true;
    proxy.proxyConfig.pushListenerPorts = String.valueOf(proxyPort);
    proxy.proxyConfig.pushFlushInterval = 10000;
    proxy.proxyConfig.pushRateLimitSourceTags = 100;
    proxy.proxyConfig.bufferFile = buffer;
    proxy.start(new String[]{});
    waitUntilListenerIsOnline(proxyPort);
    if (!(proxy.senderTaskFactory instanceof SenderTaskFactoryImpl)) fail();
    if (!(proxy.queueingFactory instanceof QueueingFactoryImpl)) fail();

    String payloadSourceTags =
        "@SourceTag action=add source=testSource addTag1 addTag2 addTag3\n" +
        "@SourceTag action=save source=testSource newtag1 newtag2\n" +
        "@SourceTag action=delete source=testSource deleteTag\n" +
        "@SourceDescription action=save source=testSource \"Long Description\"\n" +
        "@SourceDescription action=delete source=testSource";

    server.update(req -> {
      String content = req.content().toString(CharsetUtil.UTF_8);
      URI uri;
      try {
        uri = new URI(req.uri());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      String path = uri.getPath();
      logger.fine("Content received: " + content);
      switch (testCounter.incrementAndGet()) {
        case 1:
          assertEquals(HttpMethod.PUT, req.method());
          assertEquals("/api/v2/source/testSource/tag/addTag1", path);
          assertEquals("", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
        case 2:
          assertEquals(HttpMethod.PUT, req.method());
          assertEquals("/api/v2/source/testSource/tag/addTag2", path);
          assertEquals("", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
        case 3:
          assertEquals(HttpMethod.PUT, req.method());
          assertEquals("/api/v2/source/testSource/tag/addTag3", path);
          assertEquals("", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
        case 4:
          assertEquals(HttpMethod.POST, req.method());
          assertEquals("/api/v2/source/testSource/tag", path);
          assertEquals("[\"newtag1\",\"newtag2\"]", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, "");
        case 5:
          assertEquals(HttpMethod.DELETE, req.method());
          assertEquals("/api/v2/source/testSource/tag/deleteTag", path);
          assertEquals("", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
        case 6:
          assertEquals(HttpMethod.POST, req.method());
          assertEquals("/api/v2/source/testSource/description", path);
          assertEquals("Long Description", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, "");
        case 7:
          assertEquals(HttpMethod.DELETE, req.method());
          assertEquals("/api/v2/source/testSource/description", path);
          assertEquals("", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.valueOf(407), "");
        case 8:
          assertEquals(HttpMethod.POST, req.method());
          assertEquals("/api/v2/source/testSource/tag", path);
          assertEquals("[\"newtag1\",\"newtag2\"]", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
        case 9:
          assertEquals(HttpMethod.POST, req.method());
          assertEquals("/api/v2/source/testSource/description", path);
          assertEquals("Long Description", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
        case 10:
          assertEquals(HttpMethod.DELETE, req.method());
          assertEquals("/api/v2/source/testSource/description", path);
          assertEquals("", content);
          successfulSteps.incrementAndGet();
          return makeResponse(HttpResponseStatus.OK, "");
      }
      logger.warning("Too many requests");
      successfulSteps.incrementAndGet(); // this will force the assert to fail
      return makeResponse(HttpResponseStatus.OK, "");
    });
    gzippedHttpPost("http://localhost:" + proxyPort + "/", payloadSourceTags);
    HandlerKey key = HandlerKey.of(ReportableEntityType.SOURCE_TAG, String.valueOf(proxyPort));
    for (int i = 0; i < 2; i++) ((SenderTaskFactoryImpl) proxy.senderTaskFactory).flushNow(key);
    for (int i = 0; i < 4; i++) ((QueueingFactoryImpl) proxy.queueingFactory).flushNow(key);
    assertEquals(10, successfulSteps.getAndSet(0));
  }

  @Test
  public void testEndToEndHistograms() throws Exception {
    AtomicInteger successfulSteps = new AtomicInteger(0);
    AtomicInteger testCounter = new AtomicInteger(0);
    long time = (Clock.now() / 1000) / 60 * 60 + 30;
    AtomicLong digestTime = new AtomicLong(System.currentTimeMillis());
    proxyPort = findAvailablePort(2898);
    int histMinPort = findAvailablePort(40001);
    int histHourPort = findAvailablePort(40002);
    int histDayPort = findAvailablePort(40003);
    int histDistPort = findAvailablePort(40000);
    String buffer = File.createTempFile("proxyTestBuffer", null).getPath();
    proxy = new PushAgent();
    proxy.proxyConfig.server = "http://localhost:" + backendPort + "/api/";
    proxy.proxyConfig.flushThreads = 1;
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
    proxy.proxyConfig.splitPushWhenRateLimited = true;
    proxy.proxyConfig.pushListenerPorts = String.valueOf(proxyPort);
    proxy.proxyConfig.pushFlushInterval = 10000;
    proxy.proxyConfig.bufferFile = buffer;
    proxy.proxyConfig.timeProvider = digestTime::get;
    proxy.start(new String[]{});
    waitUntilListenerIsOnline(histDistPort);
    if (!(proxy.senderTaskFactory instanceof SenderTaskFactoryImpl)) fail();
    if (!(proxy.queueingFactory instanceof QueueingFactoryImpl)) fail();

    String payloadHistograms =
        "metric.name 1 " + time + " source=metric.source tagk1=tagv1\n" +
        "metric.name 1 " + time + " source=metric.source tagk1=tagv1\n" +
        "metric.name 2 " + (time + 1) + " source=metric.source tagk1=tagv1\n" +
        "metric.name 2 " + (time + 2) + " source=metric.source tagk1=tagv1\n" +
        "metric.name 3 " + time + " source=metric.source tagk1=tagv2\n" +
        "metric.name 4 " + time + " source=metric.source tagk1=tagv2\n" +
        "metric.name 5 " + (time + 60) + " source=metric.source tagk1=tagv1\n" +
        "metric.name 6 " + (time + 60) + " source=metric.source tagk1=tagv1\n";

    long minuteBin = time / 60 * 60;
    long hourBin = time / 3600 * 3600;
    long dayBin = time / 86400 * 86400;
    Set<String> expectedHistograms = ImmutableSet.of(
        "!M " + minuteBin + " #2 1.0 #2 2.0 \"metric.name\" " +
            "source=\"metric.source\" \"tagk1\"=\"tagv1\"",
        "!M " + minuteBin + " #1 3.0 #1 4.0 \"metric.name\" source=\"metric.source\" " +
            "\"tagk1\"=\"tagv2\"",
        "!M " + (minuteBin + 60) +" #1 5.0 #1 6.0 \"metric.name\" source=\"metric.source\" " +
            "\"tagk1\"=\"tagv1\"",
        "!H " + hourBin + " #2 1.0 #2 2.0 #1 5.0 #1 6.0 \"metric.name\" " +
            "source=\"metric.source\" \"tagk1\"=\"tagv1\"",
        "!H " + hourBin + " #1 3.0 #1 4.0 \"metric.name\" source=\"metric.source\" " +
            "\"tagk1\"=\"tagv2\"",
        "!D " + dayBin + " #1 3.0 #1 4.0 \"metric.name\" source=\"metric.source\" " +
            "\"tagk1\"=\"tagv2\"",
        "!D " + dayBin + " #2 1.0 #2 2.0 #1 5.0 #1 6.0 \"metric.name\" " +
            "source=\"metric.source\" \"tagk1\"=\"tagv1\"");

    String distPayload =
        "!M " + minuteBin + " #1 1.0 #1 1.0 #1 2.0 #1 2.0 \"metric.name\" " +
        "source=\"metric.source\" \"tagk1\"=\"tagv1\"\n" +
        "!M " + minuteBin + " #1 1.0 #1 1.0 #1 2.0 #1 2.0 \"metric.name\" " +
        "source=\"metric.source\" \"tagk1\"=\"tagv1\"\n" +
        "!H " + minuteBin + " #1 1.0 #1 1.0 #1 2.0 #1 2.0 \"metric.name\" " +
        "source=\"metric.source\" \"tagk1\"=\"tagv1\"\n";

    Set<String> expectedDists = ImmutableSet.of(
        "!M " + minuteBin + " #4 1.0 #4 2.0 " +
        "\"metric.name\" source=\"metric.source\" \"tagk1\"=\"tagv1\"",
        "!H " + hourBin + " #2 1.0 #2 2.0 \"metric.name\" " +
        "source=\"metric.source\" \"tagk1\"=\"tagv1\"");

    server.update(req -> {
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
      logger.fine("Content received: " + content);
      switch (testCounter.incrementAndGet()) {
        case 1:
          assertEquals(expectedHistograms, new HashSet<>(Arrays.asList(content.split("\n"))));
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
    proxy.histogramFlushRunnables.forEach(Runnable::run);
    HandlerKey key = HandlerKey.of(ReportableEntityType.HISTOGRAM, "histogram_ports");
    ((SenderTaskFactoryImpl) proxy.senderTaskFactory).flushNow(key);

    digestTime.set(System.currentTimeMillis() - 1001);
    gzippedHttpPost("http://localhost:" + histDistPort + "/", distPayload);
    digestTime.set(System.currentTimeMillis());
    proxy.histogramFlushRunnables.forEach(Runnable::run);
    ((SenderTaskFactoryImpl) proxy.senderTaskFactory).flushNow(key);
    assertEquals(2, successfulSteps.getAndSet(0));
  }

  @Test
  public void testEndToEndSpans() throws Exception {
    long time = Clock.now() / 1000;
    proxyPort = findAvailablePort(2898);
    proxyPort = findAvailablePort(2898);
    String buffer = File.createTempFile("proxyTestBuffer", null).getPath();
    proxy = new PushAgent();
    proxy.proxyConfig.server = "http://localhost:" + backendPort + "/api/";
    proxy.proxyConfig.flushThreads = 1;
    proxy.proxyConfig.traceListenerPorts = String.valueOf(proxyPort);
    proxy.proxyConfig.pushFlushInterval = 50;
    proxy.proxyConfig.bufferFile = buffer;
    proxy.proxyConfig.trafficShaping = true;
    proxy.start(new String[]{});
    waitUntilListenerIsOnline(proxyPort);
    if (!(proxy.senderTaskFactory instanceof SenderTaskFactoryImpl)) fail();
    if (!(proxy.queueingFactory instanceof QueueingFactoryImpl)) fail();

    String traceId = UUID.randomUUID().toString();
    long timestamp1 = time * 1000000 + 12345;
    long timestamp2 = time * 1000000 + 23456;
    String payload = "testSpanName parent=parent1 source=testsource spanId=testspanid " +
        "traceId=\"" + traceId + "\" parent=parent2 " + time + " " + (time + 1) + "\n" +
        "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId + "\",\"logs\":[{\"timestamp\":" +
        timestamp1 + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" +
        timestamp2 + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}]}\n";
    String expectedSpan = "\"testSpanName\" source=\"testsource\" spanId=\"testspanid\" " +
        "traceId=\"" + traceId + "\" \"parent\"=\"parent1\" \"parent\"=\"parent2\" " +
        (time * 1000) + " 1000";
    String expectedSpanLog = "{\"customer\":\"dummy\",\"traceId\":\"" + traceId + "\",\"spanId" +
        "\":\"testspanid\",\"spanSecondaryId\":null,\"logs\":[{\"timestamp\":" + timestamp1 + "," +
        "\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" + timestamp2 + "," +
        "\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}],\"span\":null}";
    AtomicBoolean gotSpan = new AtomicBoolean(false);
    AtomicBoolean gotSpanLog = new AtomicBoolean(false);
    server.update(req -> {
      String content = req.content().toString(CharsetUtil.UTF_8);
      logger.fine("Content received: " + content);
      if (content.equals(expectedSpan)) gotSpan.set(true);
      if (content.equals(expectedSpanLog)) gotSpanLog.set(true);
      return makeResponse(HttpResponseStatus.OK, "");
    });
    gzippedHttpPost("http://localhost:" + proxyPort + "/", payload);
    ((SenderTaskFactoryImpl) proxy.senderTaskFactory).
        flushNow(HandlerKey.of(ReportableEntityType.TRACE, String.valueOf(proxyPort)));
    ((SenderTaskFactoryImpl) proxy.senderTaskFactory).
        flushNow(HandlerKey.of(ReportableEntityType.TRACE_SPAN_LOGS, String.valueOf(proxyPort)));
    assertTrueWithTimeout(50, gotSpan::get);
    assertTrueWithTimeout(50, gotSpanLog::get);
  }

  @Test
  public void testEndToEndSpans_SpanLogsWithSpanField() throws Exception {
    long time = Clock.now() / 1000;
    proxyPort = findAvailablePort(2898);
    proxyPort = findAvailablePort(2898);
    String buffer = File.createTempFile("proxyTestBuffer", null).getPath();
    proxy = new PushAgent();
    proxy.proxyConfig.server = "http://localhost:" + backendPort + "/api/";
    proxy.proxyConfig.flushThreads = 1;
    proxy.proxyConfig.traceListenerPorts = String.valueOf(proxyPort);
    proxy.proxyConfig.pushFlushInterval = 50;
    proxy.proxyConfig.bufferFile = buffer;
    proxy.start(new String[]{});
    waitUntilListenerIsOnline(proxyPort);
    if (!(proxy.senderTaskFactory instanceof SenderTaskFactoryImpl)) fail();
    if (!(proxy.queueingFactory instanceof QueueingFactoryImpl)) fail();

    String traceId = UUID.randomUUID().toString();
    long timestamp1 = time * 1000000 + 12345;
    long timestamp2 = time * 1000000 + 23456;
    String payload = "testSpanName parent=parent1 source=testsource spanId=testspanid " +
        "traceId=\"" + traceId + "\" parent=parent2 " + time + " " + (time + 1) + "\n" +
        "{\"spanId\":\"testspanid\",\"traceId\":\"" + traceId + "\",\"logs\":[{\"timestamp\":" +
        timestamp1 + ",\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" +
        timestamp2 + ",\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}],\"span\":\"" +
        "testSpanName parent=parent1 source=testsource spanId=testspanid traceId=\\\"" + traceId +
        "\\\" parent=parent2 " + time + " " + (time + 1) + "\\n\"}\n";
    String expectedSpan = "\"testSpanName\" source=\"testsource\" spanId=\"testspanid\" " +
        "traceId=\"" + traceId + "\" \"parent\"=\"parent1\" \"parent\"=\"parent2\" " +
        (time * 1000) + " 1000";
    String expectedSpanLog = "{\"customer\":\"dummy\",\"traceId\":\"" + traceId + "\",\"spanId" +
        "\":\"testspanid\",\"spanSecondaryId\":null,\"logs\":[{\"timestamp\":" + timestamp1 + "," +
        "\"fields\":{\"key\":\"value\",\"key2\":\"value2\"}},{\"timestamp\":" + timestamp2 + "," +
        "\"fields\":{\"key3\":\"value3\",\"key4\":\"value4\"}}],\"span\":null}";
    AtomicBoolean gotSpan = new AtomicBoolean(false);
    AtomicBoolean gotSpanLog = new AtomicBoolean(false);
    server.update(req -> {
      String content = req.content().toString(CharsetUtil.UTF_8);
      logger.fine("Content received: " + content);
      if (content.equals(expectedSpan)) gotSpan.set(true);
      if (content.equals(expectedSpanLog)) gotSpanLog.set(true);
      return makeResponse(HttpResponseStatus.OK, "");
    });
    gzippedHttpPost("http://localhost:" + proxyPort + "/", payload);
    ((SenderTaskFactoryImpl) proxy.senderTaskFactory).
        flushNow(HandlerKey.of(ReportableEntityType.TRACE, String.valueOf(proxyPort)));
    ((SenderTaskFactoryImpl) proxy.senderTaskFactory).
        flushNow(HandlerKey.of(ReportableEntityType.TRACE_SPAN_LOGS, String.valueOf(proxyPort)));
    assertTrueWithTimeout(50, gotSpan::get);
    assertTrueWithTimeout(50, gotSpanLog::get);
  }

  private static class WrappingHttpHandler extends AbstractHttpOnlyHandler {
    private final Function<FullHttpRequest, HttpResponse> func;
    public WrappingHttpHandler(@Nullable TokenAuthenticator tokenAuthenticator,
                               @Nullable HealthCheckManager healthCheckManager,
                               @Nullable String handle,
                               @Nonnull Function<FullHttpRequest, HttpResponse> func) {
      super(tokenAuthenticator, healthCheckManager, handle);
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
      if (path.endsWith("/checkin") &&
          (path.startsWith("/api/daemon") || path.contains("wfproxy"))) {
        // simulate checkin response for proxy chaining
        ObjectNode jsonResponse = JsonNodeFactory.instance.objectNode();
        jsonResponse.put("currentTime", Clock.now());
        jsonResponse.put("allowAnyHostKeys", true);
        writeHttpResponse(ctx, HttpResponseStatus.OK, jsonResponse, request);
        return;
      } else if (path.endsWith("/config/processed")){
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
