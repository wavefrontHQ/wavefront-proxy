package com.wavefront.agent.listeners.tracing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.wavefront.agent.channel.NoopHealthCheckManager;
import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.PreprocessorRuleMetrics;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.preprocessor.SpanReplaceRegexTransformer;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.entities.tracing.sampling.DurationSampler;
import com.wavefront.sdk.entities.tracing.sampling.RateSampler;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;
import zipkin2.Endpoint;
import zipkin2.codec.SpanBytesEncoder;

import static com.wavefront.agent.TestUtils.verifyWithTimeout;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.HEART_BEAT_METRIC;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

public class ZipkinPortUnificationHandlerTest {
  private static final String DEFAULT_SOURCE = "zipkin";
  private ReportableEntityHandler<Span, String> mockTraceHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private ReportableEntityHandler<SpanLogs, String> mockTraceSpanLogsHandler =
      MockReportableEntityHandlerFactory.getMockTraceSpanLogsHandler();
  private WavefrontSender mockWavefrontSender = EasyMock.createMock(WavefrontSender.class);
  private long startTime = System.currentTimeMillis();

  // Derived RED metrics related.
  private final String PREPROCESSED_APPLICATION_TAG_VALUE = "preprocessedApplication";
  private final String PREPROCESSED_SERVICE_TAG_VALUE = "preprocessedService";
  private final String PREPROCESSED_CLUSTER_TAG_VALUE = "preprocessedCluster";
  private final String PREPROCESSED_SHARD_TAG_VALUE = "preprocessedShard";
  private final String PREPROCESSED_SOURCE_VALUE = "preprocessedSource";

  /**
   * Test for derived metrics emitted from Zipkin trace listeners. Derived metrics should report
   * tag values post applying preprocessing rules to the span.
   */
  @Test
  public void testZipkinPreprocessedDerivedMetrics() throws Exception {
    Supplier<ReportableEntityPreprocessor> preprocessorSupplier = () -> {
      ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
      PreprocessorRuleMetrics preprocessorRuleMetrics = new PreprocessorRuleMetrics(null, null,
          null);
      preprocessor.forSpan().addTransformer(new SpanReplaceRegexTransformer(APPLICATION_TAG_KEY,
          "^Zipkin.*", PREPROCESSED_APPLICATION_TAG_VALUE, null, null, false, x -> true,
          preprocessorRuleMetrics));
      preprocessor.forSpan().addTransformer(new SpanReplaceRegexTransformer(SERVICE_TAG_KEY,
          "^test.*", PREPROCESSED_SERVICE_TAG_VALUE, null, null, false, x -> true,
          preprocessorRuleMetrics));
      preprocessor.forSpan().addTransformer(new SpanReplaceRegexTransformer("sourceName",
          "^zipkin.*", PREPROCESSED_SOURCE_VALUE, null, null, false, x -> true,
          preprocessorRuleMetrics));
      preprocessor.forSpan().addTransformer(new SpanReplaceRegexTransformer(CLUSTER_TAG_KEY,
          "^none.*", PREPROCESSED_CLUSTER_TAG_VALUE, null, null, false, x -> true,
          preprocessorRuleMetrics));
      preprocessor.forSpan().addTransformer(new SpanReplaceRegexTransformer(SHARD_TAG_KEY,
          "^none.*", PREPROCESSED_SHARD_TAG_VALUE, null, null, false, x -> true,
          preprocessorRuleMetrics));
      return preprocessor;
    };

    ZipkinPortUnificationHandler handler = new ZipkinPortUnificationHandler("9411",
        new NoopHealthCheckManager(), mockTraceHandler, mockTraceSpanLogsHandler, mockWavefrontSender,
        () -> false, () -> false, preprocessorSupplier, new SpanSampler(new RateSampler(1.0D),
        false),
        null, null);

    Endpoint localEndpoint1 = Endpoint.newBuilder().serviceName("testService").ip("10.0.0.1")
        .build();
    zipkin2.Span spanServer1 = zipkin2.Span.newBuilder().
        traceId("2822889fe47043bd").
        id("2822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getservice").
        timestamp(startTime * 1000).
        duration(1234 * 1000).
        localEndpoint(localEndpoint1).
        build();

    List<zipkin2.Span> zipkinSpanList = ImmutableList.of(spanServer1);

    // Reset mock
    reset(mockTraceHandler, mockWavefrontSender);

    // Set Expectation
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime).
        setDuration(1234).
        setName("getservice").
        setSource(PREPROCESSED_SOURCE_VALUE).
        setSpanId("00000000-0000-0000-2822-889fe47043bd").
        setTraceId("00000000-0000-0000-2822-889fe47043bd").
        // Note: Order of annotations list matters for this unit test.
            setAnnotations(ImmutableList.of(
            new Annotation("zipkinSpanId", "2822889fe47043bd"),
            new Annotation("zipkinTraceId", "2822889fe47043bd"),
            new Annotation("span.kind", "server"),
            new Annotation("service", PREPROCESSED_SERVICE_TAG_VALUE),
            new Annotation("application", PREPROCESSED_APPLICATION_TAG_VALUE),
            new Annotation("cluster", PREPROCESSED_CLUSTER_TAG_VALUE),
            new Annotation("shard", PREPROCESSED_SHARD_TAG_VALUE),
            new Annotation("ipv4", "10.0.0.1"))).
            build());
    expectLastCall();

    Capture<HashMap<String, String>> tagsCapture = EasyMock.newCapture();
    mockWavefrontSender.sendMetric(eq(HEART_BEAT_METRIC), eq(1.0), anyLong(),
        eq(PREPROCESSED_SOURCE_VALUE), EasyMock.capture(tagsCapture));
    expectLastCall().anyTimes();
    replay(mockTraceHandler, mockWavefrontSender);

    ChannelHandlerContext mockCtx = createNiceMock(ChannelHandlerContext.class);
    doMockLifecycle(mockCtx);

    ByteBuf content = Unpooled.copiedBuffer(SpanBytesEncoder.JSON_V2.encodeList(zipkinSpanList));
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "http://localhost:9411/api/v2/spans",
        content,
        true
    );
    handler.handleHttpMessage(mockCtx, httpRequest);
    handler.run();

    verifyWithTimeout(500, mockTraceHandler, mockWavefrontSender);
    HashMap<String, String> tagsReturned = tagsCapture.getValue();
    assertEquals(PREPROCESSED_APPLICATION_TAG_VALUE, tagsReturned.get(APPLICATION_TAG_KEY));
    assertEquals(PREPROCESSED_SERVICE_TAG_VALUE, tagsReturned.get(SERVICE_TAG_KEY));
    assertEquals(PREPROCESSED_CLUSTER_TAG_VALUE, tagsReturned.get(CLUSTER_TAG_KEY));
    assertEquals(PREPROCESSED_SHARD_TAG_VALUE, tagsReturned.get(SHARD_TAG_KEY));
  }

  @Test
  public void testZipkinHandler() throws Exception {
    ZipkinPortUnificationHandler handler = new ZipkinPortUnificationHandler("9411",
        new NoopHealthCheckManager(), mockTraceHandler, mockTraceSpanLogsHandler, null,
        () -> false, () -> false, null, new SpanSampler(new RateSampler(1.0D), false),
        "ProxyLevelAppTag", null);

    Endpoint localEndpoint1 = Endpoint.newBuilder().serviceName("frontend").ip("10.0.0.1").build();
    zipkin2.Span spanServer1 = zipkin2.Span.newBuilder().
        traceId("2822889fe47043bd").
        id("2822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getservice").
        timestamp(startTime * 1000).
        duration(1234 * 1000).
        localEndpoint(localEndpoint1).
        putTag("http.method", "GET").
        putTag("http.url", "none+h1c://localhost:8881/").
        putTag("http.status_code", "200").
        build();

    Endpoint localEndpoint2 = Endpoint.newBuilder().serviceName("backend").ip("10.0.0.1").build();
    zipkin2.Span spanServer2 = zipkin2.Span.newBuilder().
        traceId("2822889fe47043bd").
        id("d6ab73f8a3930ae8").
        parentId("2822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getbackendservice").
        timestamp(startTime * 1000).
        duration(2234 * 1000).
        localEndpoint(localEndpoint2).
        putTag("http.method", "GET").
        putTag("http.url", "none+h2c://localhost:9000/api").
        putTag("http.status_code", "200").
        putTag("component", "jersey-server").
        putTag("application", "SpanLevelAppTag").
        addAnnotation(startTime * 1000, "start processing").
        build();

    zipkin2.Span spanServer3 = zipkin2.Span.newBuilder().
            traceId("2822889fe47043bd").
            id("d6ab73f8a3930ae8").
            kind(zipkin2.Span.Kind.CLIENT).
            name("getbackendservice2").
            timestamp(startTime * 1000).
            duration(2234 * 1000).
            localEndpoint(localEndpoint2).
            putTag("http.method", "GET").
            putTag("http.url", "none+h2c://localhost:9000/api").
            putTag("http.status_code", "200").
            putTag("component", "jersey-server").
            putTag("application", "SpanLevelAppTag").
            putTag("emptry.tag", "").
            addAnnotation(startTime * 1000, "start processing").
            build();

    List<zipkin2.Span> zipkinSpanList = ImmutableList.of(spanServer1, spanServer2, spanServer3);

    // Validate all codecs i.e. JSON_V1, JSON_V2, THRIFT and PROTO3.
    for (SpanBytesEncoder encoder : SpanBytesEncoder.values()) {
      ByteBuf content = Unpooled.copiedBuffer(encoder.encodeList(zipkinSpanList));
      // take care of mocks.
      doMockLifecycle(mockTraceHandler, mockTraceSpanLogsHandler);
      ChannelHandlerContext mockCtx = createNiceMock(ChannelHandlerContext.class);
      doMockLifecycle(mockCtx);
      FullHttpRequest httpRequest = new DefaultFullHttpRequest(
          HttpVersion.HTTP_1_1,
          HttpMethod.POST,
          "http://localhost:9411/api/v1/spans",
          content,
          true
      );
      handler.handleHttpMessage(mockCtx, httpRequest);
      verify(mockTraceHandler, mockTraceSpanLogsHandler);
    }
  }

  private void doMockLifecycle(ChannelHandlerContext mockCtx) {
    reset(mockCtx);
    EasyMock.expect(mockCtx.write(EasyMock.isA(FullHttpResponse.class))).andReturn(null);
    EasyMock.replay(mockCtx);
  }

  private void doMockLifecycle(ReportableEntityHandler<Span, String> mockTraceHandler,
                               ReportableEntityHandler<SpanLogs, String> mockTraceSpanLogsHandler) {
    // Reset mock
    reset(mockTraceHandler, mockTraceSpanLogsHandler);

    // Set Expectation
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime).
        setDuration(1234).
        setName("getservice").
        setSource(DEFAULT_SOURCE).
        setSpanId("00000000-0000-0000-2822-889fe47043bd").
        setTraceId("00000000-0000-0000-2822-889fe47043bd").
        // Note: Order of annotations list matters for this unit test.
        setAnnotations(ImmutableList.of(
            new Annotation("zipkinSpanId", "2822889fe47043bd"),
            new Annotation("zipkinTraceId", "2822889fe47043bd"),
            new Annotation("span.kind", "server"),
            new Annotation("service", "frontend"),
            new Annotation("http.method", "GET"),
            new Annotation("http.status_code", "200"),
            new Annotation("http.url", "none+h1c://localhost:8881/"),
            new Annotation("application", "ProxyLevelAppTag"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("ipv4", "10.0.0.1"))).
        build());
    expectLastCall();

    Span span1 = Span.newBuilder().setCustomer("dummy").setStartMillis(startTime).
        setDuration(2234).
        setName("getbackendservice").
        setSource(DEFAULT_SOURCE).
        setSpanId("00000000-0000-0000-d6ab-73f8a3930ae8").
        setTraceId("00000000-0000-0000-2822-889fe47043bd").
        // Note: Order of annotations list matters for this unit test.
        setAnnotations(ImmutableList.of(
            new Annotation("zipkinSpanId", "d6ab73f8a3930ae8"),
            new Annotation("zipkinTraceId", "2822889fe47043bd"),
            new Annotation("parent", "00000000-0000-0000-2822-889fe47043bd"),
            new Annotation("span.kind", "server"),
            new Annotation("_spanSecondaryId", "server"),
            new Annotation("service", "backend"),
            new Annotation("component", "jersey-server"),
            new Annotation("http.method", "GET"),
            new Annotation("http.status_code", "200"),
            new Annotation("http.url", "none+h2c://localhost:9000/api"),
            new Annotation("application", "SpanLevelAppTag"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("ipv4", "10.0.0.1"),
            new Annotation("_spanLogs", "true"))).
        build();
    mockTraceHandler.report(span1);
    expectLastCall();

    Span span2 = Span.newBuilder().setCustomer("dummy").setStartMillis(startTime).
        setDuration(2234).
        setName("getbackendservice2").
        setSource(DEFAULT_SOURCE).
        setTraceId("00000000-0000-0000-2822-889fe47043bd").
        setSpanId("00000000-0000-0000-d6ab-73f8a3930ae8").
        // Note: Order of annotations list matters for this unit test.
        setAnnotations(ImmutableList.of(
            new Annotation("zipkinSpanId", "d6ab73f8a3930ae8"),
            new Annotation("zipkinTraceId", "2822889fe47043bd"),
            new Annotation("span.kind", "client"),
            new Annotation("_spanSecondaryId", "client"),
            new Annotation("service", "backend"),
            new Annotation("component", "jersey-server"),
            new Annotation("http.method", "GET"),
            new Annotation("http.status_code", "200"),
            new Annotation("http.url", "none+h2c://localhost:9000/api"),
            new Annotation("application", "SpanLevelAppTag"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("ipv4", "10.0.0.1"),
            new Annotation("_spanLogs", "true"))).
        build();
    mockTraceHandler.report(span2);
    expectLastCall();

    mockTraceSpanLogsHandler.report(SpanLogs.newBuilder().
        setCustomer("default").
        setTraceId("00000000-0000-0000-2822-889fe47043bd").
        setSpanId("00000000-0000-0000-d6ab-73f8a3930ae8").
        setSpanSecondaryId("server").
        setLogs(ImmutableList.of(
            SpanLog.newBuilder().
                setTimestamp(startTime * 1000).
                setFields(ImmutableMap.of("annotation", "start processing")).
                build()
            )).
        build());
    expectLastCall();

    mockTraceSpanLogsHandler.report(SpanLogs.newBuilder().
        setCustomer("default").
        setTraceId("00000000-0000-0000-2822-889fe47043bd").
        setSpanId("00000000-0000-0000-d6ab-73f8a3930ae8").
        setSpanSecondaryId("client").
        setLogs(ImmutableList.of(
                SpanLog.newBuilder().
                        setTimestamp(startTime * 1000).
                        setFields(ImmutableMap.of("annotation", "start processing")).
                        build()
        )).
        build());
    expectLastCall();

    // Replay
    replay(mockTraceHandler, mockTraceSpanLogsHandler);
  }

  @Test
  public void testZipkinDurationSampler() throws Exception {
    ZipkinPortUnificationHandler handler = new ZipkinPortUnificationHandler("9411",
        new NoopHealthCheckManager(), mockTraceHandler, mockTraceSpanLogsHandler, null,
        () -> false, () -> false, null, new SpanSampler(new DurationSampler(5), false), null, null);

    Endpoint localEndpoint1 = Endpoint.newBuilder().serviceName("frontend").ip("10.0.0.1").build();
    zipkin2.Span spanServer1 = zipkin2.Span.newBuilder().
        traceId("2822889fe47043bd").
        id("2822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getservice").
        timestamp(startTime * 1000).
        duration(4 * 1000).
        localEndpoint(localEndpoint1).
        putTag("http.method", "GET").
        putTag("http.url", "none+h1c://localhost:8881/").
        putTag("http.status_code", "200").
        addAnnotation(startTime * 1000, "start processing").
        build();

    zipkin2.Span spanServer2 = zipkin2.Span.newBuilder().
        traceId("3822889fe47043bd").
        id("3822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getservice").
        timestamp(startTime * 1000).
        duration(9 * 1000).
        localEndpoint(localEndpoint1).
        putTag("http.method", "GET").
        putTag("http.url", "none+h1c://localhost:8881/").
        putTag("http.status_code", "200").
        addAnnotation(startTime * 1000, "start processing").
        build();

    List<zipkin2.Span> zipkinSpanList = ImmutableList.of(spanServer1, spanServer2);

    SpanBytesEncoder encoder = SpanBytesEncoder.values()[1];
    ByteBuf content = Unpooled.copiedBuffer(encoder.encodeList(zipkinSpanList));
    // take care of mocks.
    // Reset mock
    reset(mockTraceHandler, mockTraceSpanLogsHandler);

    // Set Expectation
    Span expectedSpan2 = Span.newBuilder().setCustomer("dummy").setStartMillis(startTime).
        setDuration(9).
        setName("getservice").
        setSource(DEFAULT_SOURCE).
        setSpanId("00000000-0000-0000-3822-889fe47043bd").
        setTraceId("00000000-0000-0000-3822-889fe47043bd").
        // Note: Order of annotations list matters for this unit test.
        setAnnotations(ImmutableList.of(
            new Annotation("zipkinSpanId", "3822889fe47043bd"),
            new Annotation("zipkinTraceId", "3822889fe47043bd"),
            new Annotation("span.kind", "server"),
            new Annotation("_spanSecondaryId", "server"),
            new Annotation("service", "frontend"),
            new Annotation("http.method", "GET"),
            new Annotation("http.status_code", "200"),
            new Annotation("http.url", "none+h1c://localhost:8881/"),
            new Annotation("application", "Zipkin"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("ipv4", "10.0.0.1"),
            new Annotation("_spanLogs", "true"))).
        build();
    mockTraceHandler.report(expectedSpan2);
    expectLastCall();
    mockTraceSpanLogsHandler.report(SpanLogs.newBuilder().
        setCustomer("default").
        setTraceId("00000000-0000-0000-3822-889fe47043bd").
        setSpanId("00000000-0000-0000-3822-889fe47043bd").
        setSpanSecondaryId("server").
        setLogs(ImmutableList.of(
            SpanLog.newBuilder().
                setTimestamp(startTime * 1000).
                setFields(ImmutableMap.of("annotation", "start processing")).
                build()
        )).
        build());
    expectLastCall();
    replay(mockTraceHandler, mockTraceSpanLogsHandler);

    ChannelHandlerContext mockCtx = createNiceMock(ChannelHandlerContext.class);
    doMockLifecycle(mockCtx);
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "http://localhost:9411/api/v1/spans",
        content,
        true
    );
    handler.handleHttpMessage(mockCtx, httpRequest);
    verify(mockTraceHandler, mockTraceSpanLogsHandler);
  }

  @Test
  public void testZipkinDebugOverride() throws Exception {
    ZipkinPortUnificationHandler handler = new ZipkinPortUnificationHandler("9411",
        new NoopHealthCheckManager(), mockTraceHandler, mockTraceSpanLogsHandler, null,
        () -> false, () -> false, null, new SpanSampler(new DurationSampler(10), false), null,
        null);

    // take care of mocks.
    // Reset mock
    reset(mockTraceHandler, mockTraceSpanLogsHandler);

    // Set Expectation
    Span expectedSpan2 = Span.newBuilder().setCustomer("dummy").setStartMillis(startTime).
        setDuration(9).
        setName("getservice").
        setSource(DEFAULT_SOURCE).
        setSpanId("00000000-0000-0000-3822-889fe47043bd").
        setTraceId("00000000-0000-0000-3822-889fe47043bd").
        // Note: Order of annotations list matters for this unit test.
        setAnnotations(ImmutableList.of(
            new Annotation("zipkinSpanId", "3822889fe47043bd"),
            new Annotation("zipkinTraceId", "3822889fe47043bd"),
            new Annotation("span.kind", "server"),
            new Annotation("_spanSecondaryId", "server"),
            new Annotation("service", "frontend"),
            new Annotation("http.method", "GET"),
            new Annotation("http.status_code", "200"),
            new Annotation("http.url", "none+h1c://localhost:8881/"),
            new Annotation("application", "Zipkin"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("debug", "true"),
            new Annotation("ipv4", "10.0.0.1"),
            new Annotation("_spanLogs", "true"))).
        build();
    mockTraceHandler.report(expectedSpan2);
    expectLastCall();
    mockTraceSpanLogsHandler.report(SpanLogs.newBuilder().
        setCustomer("default").
        setTraceId("00000000-0000-0000-3822-889fe47043bd").
        setSpanId("00000000-0000-0000-3822-889fe47043bd").
        setSpanSecondaryId("server").
        setLogs(ImmutableList.of(
            SpanLog.newBuilder().
                setTimestamp(startTime * 1000).
                setFields(ImmutableMap.of("annotation", "start processing")).
                build()
        )).
        build());
    expectLastCall();

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime).
        setDuration(6).
        setName("getservice").
        setSource(DEFAULT_SOURCE).
        setSpanId("00000000-0000-0000-5822-889fe47043bd").
        setTraceId("00000000-0000-0000-5822-889fe47043bd").
        // Note: Order of annotations list matters for this unit test.
        setAnnotations(ImmutableList.of(
            new Annotation("zipkinSpanId", "5822889fe47043bd"),
            new Annotation("zipkinTraceId", "5822889fe47043bd"),
            new Annotation("span.kind", "server"),
            new Annotation("service", "frontend"),
            new Annotation("debug", "debug-id-4"),
            new Annotation("http.method", "GET"),
            new Annotation("http.status_code", "200"),
            new Annotation("http.url", "none+h1c://localhost:8881/"),
            new Annotation("application", "Zipkin"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("debug", "true"),
            new Annotation("ipv4", "10.0.0.1"))).
        build());
    expectLastCall();

    Endpoint localEndpoint1 = Endpoint.newBuilder().serviceName("frontend").ip("10.0.0.1").build();
    zipkin2.Span spanServer1 = zipkin2.Span.newBuilder().
        traceId("2822889fe47043bd").
        id("2822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getservice").
        timestamp(startTime * 1000).
        duration(8 * 1000).
        localEndpoint(localEndpoint1).
        putTag("http.method", "GET").
        putTag("http.url", "none+h1c://localhost:8881/").
        putTag("http.status_code", "200").
        addAnnotation(startTime * 1000, "start processing").
        build();

    zipkin2.Span spanServer2 = zipkin2.Span.newBuilder().
        traceId("3822889fe47043bd").
        id("3822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getservice").
        timestamp(startTime * 1000).
        duration(9 * 1000).
        localEndpoint(localEndpoint1).
        putTag("http.method", "GET").
        putTag("http.url", "none+h1c://localhost:8881/").
        putTag("http.status_code", "200").
        debug(true).
        addAnnotation(startTime * 1000, "start processing").
        build();

    zipkin2.Span spanServer3 = zipkin2.Span.newBuilder().
        traceId("4822889fe47043bd").
        id("4822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getservice").
        timestamp(startTime * 1000).
        duration(7 * 1000).
        localEndpoint(localEndpoint1).
        putTag("http.method", "GET").
        putTag("http.url", "none+h1c://localhost:8881/").
        putTag("http.status_code", "200").
        putTag("debug", "debug-id-1").
        addAnnotation(startTime * 1000, "start processing").
        build();

    zipkin2.Span spanServer4 = zipkin2.Span.newBuilder().
        traceId("5822889fe47043bd").
        id("5822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getservice").
        timestamp(startTime * 1000).
        duration(6 * 1000).
        localEndpoint(localEndpoint1).
        putTag("http.method", "GET").
        putTag("http.url", "none+h1c://localhost:8881/").
        putTag("http.status_code", "200").
        putTag("debug", "debug-id-4").
        debug(true).
        build();

    List<zipkin2.Span> zipkinSpanList = ImmutableList.of(spanServer1, spanServer2, spanServer3,
        spanServer4);

    SpanBytesEncoder encoder = SpanBytesEncoder.values()[1];
    ByteBuf content = Unpooled.copiedBuffer(encoder.encodeList(zipkinSpanList));

    replay(mockTraceHandler, mockTraceSpanLogsHandler);

    ChannelHandlerContext mockCtx = createNiceMock(ChannelHandlerContext.class);
    doMockLifecycle(mockCtx);
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "http://localhost:9411/api/v1/spans",
        content,
        true
    );
    handler.handleHttpMessage(mockCtx, httpRequest);
    verify(mockTraceHandler, mockTraceSpanLogsHandler);
  }

  @Test
  public void testZipkinCustomSource() throws Exception {
    ZipkinPortUnificationHandler handler = new ZipkinPortUnificationHandler("9411",
        new NoopHealthCheckManager(), mockTraceHandler, mockTraceSpanLogsHandler, null,
        () -> false, () -> false, null, new SpanSampler(new RateSampler(1.0D), false), null, null);

    // take care of mocks.
    // Reset mock
    reset(mockTraceHandler, mockTraceSpanLogsHandler);

    // Set Expectation
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime).
        setDuration(9).
        setName("getservice").
        setSource("customZipkinSource").
        setSpanId("00000000-0000-0000-2822-889fe47043bd").
        setTraceId("00000000-0000-0000-2822-889fe47043bd").
        // Note: Order of annotations list matters for this unit test.
        setAnnotations(ImmutableList.of(
            new Annotation("zipkinSpanId", "2822889fe47043bd"),
            new Annotation("zipkinTraceId", "2822889fe47043bd"),
            new Annotation("span.kind", "server"),
            new Annotation("service", "frontend"),
            new Annotation("http.method", "GET"),
            new Annotation("http.status_code", "200"),
            new Annotation("http.url", "none+h1c://localhost:8881/"),
            new Annotation("application", "Zipkin"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("ipv4", "10.0.0.1"))).
        build());
    expectLastCall();

    Endpoint localEndpoint1 = Endpoint.newBuilder().serviceName("frontend").ip("10.0.0.1").build();
    zipkin2.Span spanServer1 = zipkin2.Span.newBuilder().
        traceId("2822889fe47043bd").
        id("2822889fe47043bd").
        kind(zipkin2.Span.Kind.SERVER).
        name("getservice").
        timestamp(startTime * 1000).
        duration(9 * 1000).
        localEndpoint(localEndpoint1).
        putTag("http.method", "GET").
        putTag("http.url", "none+h1c://localhost:8881/").
        putTag("http.status_code", "200").
        putTag("source", "customZipkinSource").
        build();

    List<zipkin2.Span> zipkinSpanList = ImmutableList.of(spanServer1);

    SpanBytesEncoder encoder = SpanBytesEncoder.values()[1];
    ByteBuf content = Unpooled.copiedBuffer(encoder.encodeList(zipkinSpanList));

    replay(mockTraceHandler, mockTraceSpanLogsHandler);

    ChannelHandlerContext mockCtx = createNiceMock(ChannelHandlerContext.class);
    doMockLifecycle(mockCtx);
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "http://localhost:9411/api/v1/spans",
        content,
        true
    );
    handler.handleHttpMessage(mockCtx, httpRequest);
    verify(mockTraceHandler, mockTraceSpanLogsHandler);
  }
}
