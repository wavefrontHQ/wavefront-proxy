package com.wavefront.agent.listeners.tracing;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.tchannel.messages.ThriftRequest;
import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.api.agent.SpanSamplingPolicy;
import com.wavefront.sdk.entities.tracing.sampling.DurationSampler;
import com.wavefront.sdk.entities.tracing.sampling.RateSampler;
import io.jaegertracing.thriftjava.Batch;
import io.jaegertracing.thriftjava.Collector;
import io.jaegertracing.thriftjava.Log;
import io.jaegertracing.thriftjava.Process;
import io.jaegertracing.thriftjava.Tag;
import io.jaegertracing.thriftjava.TagType;
import org.easymock.Capture;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

public class JaegerTChannelCollectorHandlerTest {
  private static final String DEFAULT_SOURCE = "jaeger";
  private ReportableEntityHandler<Span, String> mockTraceHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private ReportableEntityHandler<SpanLogs, String> mockTraceLogsHandler =
      MockReportableEntityHandlerFactory.getMockTraceSpanLogsHandler();
  private long startTime = System.currentTimeMillis();

  @Test
  public void testJaegerTChannelCollector() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);
    Span expectedSpan1 =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(1234)
            .setName("HTTP GET")
            .setSource(DEFAULT_SOURCE)
            .setSpanId("00000000-0000-0000-0000-00000012d687")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "12d687"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("component", "db"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none"),
                    new Annotation("_spanLogs", "true")))
            .build();
    mockTraceHandler.report(expectedSpan1);
    expectLastCall();

    mockTraceLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("default")
            .setSpanId("00000000-0000-0000-0000-00000012d687")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            .setSpan("_sampledByPolicy=NONE")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(startTime * 1000)
                        .setFields(
                            ImmutableMap.of("event", "error", "exception", "NullPointerException"))
                        .build()))
            .build());
    expectLastCall();

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(2345)
            .setName("HTTP GET /")
            .setSource(DEFAULT_SOURCE)
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "23cace"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("parent", "00000000-0000-0000-0000-00000012d687"),
                    new Annotation("component", "db"),
                    new Annotation("application", "Custom-JaegerApp"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(3456)
            .setName("HTTP GET /")
            .setSource(DEFAULT_SOURCE)
            .setSpanId("00000000-0000-0000-9a12-b85901d53397")
            .setTraceId("00000000-0000-0000-fea4-87ee36e58cab")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "9a12b85901d53397"),
                    new Annotation("jaegerTraceId", "fea487ee36e58cab"),
                    new Annotation("service", "frontend"),
                    new Annotation("parent", "00000000-0000-0000-fea4-87ee36e58cab"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();

    // Test filtering empty tags
    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(3456)
            .setName("HTTP GET /test")
            .setSource(DEFAULT_SOURCE)
            .setSpanId("00000000-0000-0000-0000-0051759bfc69")
            .setTraceId("0000011e-ab2a-9944-0000-000049631900")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "51759bfc69"),
                    new Annotation("jaegerTraceId", "11eab2a99440000000049631900"),
                    new Annotation("service", "frontend"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();

    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerTChannelCollectorHandler handler =
        new JaegerTChannelCollectorHandler(
            "9876",
            mockTraceHandler,
            mockTraceLogsHandler,
            null,
            () -> false,
            () -> false,
            null,
            new SpanSampler(new RateSampler(1.0D), () -> null),
            null,
            null);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    Tag componentTag = new Tag("component", TagType.STRING);
    componentTag.setVStr("db");

    Tag customApplicationTag = new Tag("application", TagType.STRING);
    customApplicationTag.setVStr("Custom-JaegerApp");

    Tag emptyTag = new Tag("empty", TagType.STRING);
    emptyTag.setVStr("");

    io.jaegertracing.thriftjava.Span span1 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L,
            1234567890L,
            1234567L,
            0L,
            "HTTP GET",
            1,
            startTime * 1000,
            1234 * 1000);

    io.jaegertracing.thriftjava.Span span2 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L,
            1234567890L,
            2345678L,
            1234567L,
            "HTTP GET /",
            1,
            startTime * 1000,
            2345 * 1000);

    // check negative span IDs too
    io.jaegertracing.thriftjava.Span span3 =
        new io.jaegertracing.thriftjava.Span(
            -97803834702328661L,
            0L,
            -7344605349865507945L,
            -97803834702328661L,
            "HTTP GET /",
            1,
            startTime * 1000,
            3456 * 1000);

    io.jaegertracing.thriftjava.Span span4 =
        new io.jaegertracing.thriftjava.Span(
            1231231232L,
            1231232342340L,
            349865507945L,
            0,
            "HTTP GET /test",
            1,
            startTime * 1000,
            3456 * 1000);

    span1.setTags(ImmutableList.of(componentTag));
    span2.setTags(ImmutableList.of(componentTag, customApplicationTag));
    span4.setTags(ImmutableList.of(emptyTag));

    Tag tag1 = new Tag("event", TagType.STRING);
    tag1.setVStr("error");
    Tag tag2 = new Tag("exception", TagType.STRING);
    tag2.setVStr("NullPointerException");
    span1.setLogs(ImmutableList.of(new Log(startTime * 1000, ImmutableList.of(tag1, tag2))));

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    testBatch.process.setTags(ImmutableList.of(ipTag));

    testBatch.setSpans(ImmutableList.of(span1, span2, span3, span4));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batches)
            .build();
    handler.handleImpl(request);

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testApplicationTagPriority() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);

    // Span to verify span level tags precedence
    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(1234)
            .setName("HTTP GET")
            .setSource(DEFAULT_SOURCE)
            .setSpanId("00000000-0000-0000-0000-00000012d687")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "12d687"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("component", "db"),
                    new Annotation("application", "SpanLevelAppTag"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();

    // Span to verify process level tags precedence
    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(2345)
            .setName("HTTP GET /")
            .setSource(DEFAULT_SOURCE)
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "23cace"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("parent", "00000000-0000-0000-0000-00000012d687"),
                    new Annotation("component", "db"),
                    new Annotation("application", "ProcessLevelAppTag"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();

    // Span to verify Proxy level tags precedence
    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(3456)
            .setName("HTTP GET /")
            .setSource(DEFAULT_SOURCE)
            .setSpanId("00000000-0000-0000-9a12-b85901d53397")
            .setTraceId("00000000-0000-0000-fea4-87ee36e58cab")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "9a12b85901d53397"),
                    new Annotation("jaegerTraceId", "fea487ee36e58cab"),
                    new Annotation("service", "frontend"),
                    new Annotation("parent", "00000000-0000-0000-fea4-87ee36e58cab"),
                    new Annotation("application", "ProxyLevelAppTag"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();
    replay(mockTraceHandler, mockTraceLogsHandler);

    // Verify span level "application" tags precedence
    JaegerTChannelCollectorHandler handler =
        new JaegerTChannelCollectorHandler(
            "9876",
            mockTraceHandler,
            mockTraceLogsHandler,
            null,
            () -> false,
            () -> false,
            null,
            new SpanSampler(new RateSampler(1.0D), () -> null),
            "ProxyLevelAppTag",
            null);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    Tag componentTag = new Tag("component", TagType.STRING);
    componentTag.setVStr("db");

    Tag spanLevelAppTag = new Tag("application", TagType.STRING);
    spanLevelAppTag.setVStr("SpanLevelAppTag");

    Tag processLevelAppTag = new Tag("application", TagType.STRING);
    processLevelAppTag.setVStr("ProcessLevelAppTag");

    io.jaegertracing.thriftjava.Span span1 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L,
            1234567890L,
            1234567L,
            0L,
            "HTTP GET",
            1,
            startTime * 1000,
            1234 * 1000);

    io.jaegertracing.thriftjava.Span span2 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L,
            1234567890L,
            2345678L,
            1234567L,
            "HTTP GET /",
            1,
            startTime * 1000,
            2345 * 1000);

    // check negative span IDs too
    io.jaegertracing.thriftjava.Span span3 =
        new io.jaegertracing.thriftjava.Span(
            -97803834702328661L,
            0L,
            -7344605349865507945L,
            -97803834702328661L,
            "HTTP GET /",
            1,
            startTime * 1000,
            3456 * 1000);

    // Span1 to verify span level tags precedence
    span1.setTags(ImmutableList.of(componentTag, spanLevelAppTag));
    span2.setTags(ImmutableList.of(componentTag));

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    // Span2 to verify process level tags precedence
    testBatch.process.setTags(ImmutableList.of(ipTag, processLevelAppTag));

    testBatch.setSpans(ImmutableList.of(span1, span2));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batches)
            .build();
    handler.handleImpl(request);

    // Span3 to verify process level tags precedence. So do not set any process level tag.
    Batch testBatchForProxyLevel = new Batch();
    testBatchForProxyLevel.process = new Process();
    testBatchForProxyLevel.process.serviceName = "frontend";
    testBatchForProxyLevel.process.setTags(ImmutableList.of(ipTag));

    testBatchForProxyLevel.setSpans(ImmutableList.of(span3));

    Collector.submitBatches_args batchesForProxyLevel = new Collector.submitBatches_args();
    batchesForProxyLevel.addToBatches(testBatchForProxyLevel);
    ThriftRequest<Collector.submitBatches_args> requestForProxyLevel =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batchesForProxyLevel)
            .build();
    handler.handleImpl(requestForProxyLevel);

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testJaegerDurationSampler() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);

    Span expectedSpan2 =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(9)
            .setName("HTTP GET /")
            .setSource(DEFAULT_SOURCE)
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "23cace"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("parent", "00000000-0000-0000-0000-00000012d687"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none"),
                    new Annotation("_spanLogs", "true")))
            .build();
    mockTraceHandler.report(expectedSpan2);
    expectLastCall();

    mockTraceLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("default")
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            .setSpan("_sampledByPolicy=NONE")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(startTime * 1000)
                        .setFields(
                            ImmutableMap.of("event", "error", "exception", "NullPointerException"))
                        .build()))
            .build());
    expectLastCall();

    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerTChannelCollectorHandler handler =
        new JaegerTChannelCollectorHandler(
            "9876",
            mockTraceHandler,
            mockTraceLogsHandler,
            null,
            () -> false,
            () -> false,
            null,
            new SpanSampler(new DurationSampler(5), () -> null),
            null,
            null);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    io.jaegertracing.thriftjava.Span span1 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L, 1234567890L, 1234567L, 0L, "HTTP GET", 1, startTime * 1000, 4 * 1000);

    io.jaegertracing.thriftjava.Span span2 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L,
            1234567890L,
            2345678L,
            1234567L,
            "HTTP GET /",
            1,
            startTime * 1000,
            9 * 1000);

    Tag tag1 = new Tag("event", TagType.STRING);
    tag1.setVStr("error");
    Tag tag2 = new Tag("exception", TagType.STRING);
    tag2.setVStr("NullPointerException");
    span1.setLogs(ImmutableList.of(new Log(startTime * 1000, ImmutableList.of(tag1, tag2))));
    span2.setLogs(ImmutableList.of(new Log(startTime * 1000, ImmutableList.of(tag1, tag2))));

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    testBatch.process.setTags(ImmutableList.of(ipTag));

    testBatch.setSpans(ImmutableList.of(span1, span2));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batches)
            .build();
    handler.handleImpl(request);

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testJaegerDebugOverride() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);

    Span expectedSpan1 =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(9)
            .setName("HTTP GET /")
            .setSource(DEFAULT_SOURCE)
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "23cace"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("parent", "00000000-0000-0000-0000-00000012d687"),
                    new Annotation("debug", "true"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none"),
                    new Annotation("_spanLogs", "true")))
            .build();
    mockTraceHandler.report(expectedSpan1);
    expectLastCall();

    mockTraceLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("default")
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            .setSpan("_sampledByPolicy=NONE")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(startTime * 1000)
                        .setFields(
                            ImmutableMap.of("event", "error", "exception", "NullPointerException"))
                        .build()))
            .build());
    expectLastCall();

    Span expectedSpan2 =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(4)
            .setName("HTTP GET")
            .setSource(DEFAULT_SOURCE)
            .setSpanId("00000000-0000-0000-0000-00000012d687")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "12d687"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("sampling.priority", "0.3"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none"),
                    new Annotation("_spanLogs", "true"),
                    new Annotation("_sampledByPolicy", "test")))
            .build();
    mockTraceHandler.report(expectedSpan2);
    expectLastCall();

    mockTraceLogsHandler.report(
        SpanLogs.newBuilder()
            .setCustomer("default")
            .setSpanId("00000000-0000-0000-0000-00000012d687")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            .setSpan("_sampledByPolicy=test")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setTimestamp(startTime * 1000)
                        .setFields(
                            ImmutableMap.of("event", "error", "exception", "NullPointerException"))
                        .build()))
            .build());
    expectLastCall();

    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerTChannelCollectorHandler handler =
        new JaegerTChannelCollectorHandler(
            "9876",
            mockTraceHandler,
            mockTraceLogsHandler,
            null,
            () -> false,
            () -> false,
            null,
            new SpanSampler(
                new DurationSampler(10),
                () ->
                    ImmutableList.of(
                        new SpanSamplingPolicy("test", "{{sampling.priority}}='0.3'", 100))),
            null,
            null);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    Tag debugTag = new Tag("debug", TagType.STRING);
    debugTag.setVStr("true");

    io.jaegertracing.thriftjava.Span span1 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L,
            1234567890L,
            2345678L,
            1234567L,
            "HTTP GET /",
            1,
            startTime * 1000,
            9 * 1000);
    span1.setTags(ImmutableList.of(debugTag));

    Tag samplePriorityTag = new Tag("sampling.priority", TagType.DOUBLE);
    samplePriorityTag.setVDouble(0.3);
    io.jaegertracing.thriftjava.Span span2 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L, 1234567890L, 1234567L, 0L, "HTTP GET", 1, startTime * 1000, 4 * 1000);
    span2.setTags(ImmutableList.of(samplePriorityTag));

    Tag tag1 = new Tag("event", TagType.STRING);
    tag1.setVStr("error");
    Tag tag2 = new Tag("exception", TagType.STRING);
    tag2.setVStr("NullPointerException");
    span1.setLogs(ImmutableList.of(new Log(startTime * 1000, ImmutableList.of(tag1, tag2))));
    span2.setLogs(ImmutableList.of(new Log(startTime * 1000, ImmutableList.of(tag1, tag2))));

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    testBatch.process.setTags(ImmutableList.of(ipTag));

    testBatch.setSpans(ImmutableList.of(span1, span2));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batches)
            .build();
    handler.handleImpl(request);

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testSourceTagPriority() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(9)
            .setName("HTTP GET /")
            .setSource("source-spantag")
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "23cace"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("parent", "00000000-0000-0000-0000-00000012d687"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(4)
            .setName("HTTP GET")
            .setSource("source-processtag")
            .setSpanId("00000000-0000-0000-0000-00000012d687")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "12d687"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(3456)
            .setName("HTTP GET /test")
            .setSource("hostname-processtag")
            .setSpanId("00000000-0000-0000-0000-0051759bfc69")
            .setTraceId("0000011e-ab2a-9944-0000-000049631900")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "51759bfc69"),
                    new Annotation("jaegerTraceId", "11eab2a99440000000049631900"),
                    new Annotation("service", "frontend"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();
    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerTChannelCollectorHandler handler =
        new JaegerTChannelCollectorHandler(
            "9876",
            mockTraceHandler,
            mockTraceLogsHandler,
            null,
            () -> false,
            () -> false,
            null,
            new SpanSampler(new RateSampler(1.0D), () -> null),
            null,
            null);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    Tag hostNameProcessTag = new Tag("hostname", TagType.STRING);
    hostNameProcessTag.setVStr("hostname-processtag");

    Tag customSourceProcessTag = new Tag("source", TagType.STRING);
    customSourceProcessTag.setVStr("source-processtag");

    Tag customSourceSpanTag = new Tag("source", TagType.STRING);
    customSourceSpanTag.setVStr("source-spantag");

    io.jaegertracing.thriftjava.Span span1 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L,
            1234567890L,
            2345678L,
            1234567L,
            "HTTP GET /",
            1,
            startTime * 1000,
            9 * 1000);
    span1.setTags(ImmutableList.of(customSourceSpanTag));

    io.jaegertracing.thriftjava.Span span2 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L, 1234567890L, 1234567L, 0L, "HTTP GET", 1, startTime * 1000, 4 * 1000);

    io.jaegertracing.thriftjava.Span span3 =
        new io.jaegertracing.thriftjava.Span(
            1231231232L,
            1231232342340L,
            349865507945L,
            0,
            "HTTP GET /test",
            1,
            startTime * 1000,
            3456 * 1000);

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    testBatch.process.setTags(ImmutableList.of(ipTag, hostNameProcessTag, customSourceProcessTag));

    testBatch.setSpans(ImmutableList.of(span1, span2));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batches)
            .build();
    handler.handleImpl(request);

    // Span3 to verify hostname process level tags precedence. So do not set any process level
    // source tag.
    Batch testBatchSourceAsProcessTagHostName = new Batch();
    testBatchSourceAsProcessTagHostName.process = new Process();
    testBatchSourceAsProcessTagHostName.process.serviceName = "frontend";
    testBatchSourceAsProcessTagHostName.process.setTags(
        ImmutableList.of(ipTag, hostNameProcessTag));

    testBatchSourceAsProcessTagHostName.setSpans(ImmutableList.of(span3));

    Collector.submitBatches_args batchesSourceAsProcessTagHostName =
        new Collector.submitBatches_args();
    batchesSourceAsProcessTagHostName.addToBatches(testBatchSourceAsProcessTagHostName);
    ThriftRequest<Collector.submitBatches_args> requestForProxyLevel =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batchesSourceAsProcessTagHostName)
            .build();
    handler.handleImpl(requestForProxyLevel);

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testIgnoresServiceTags() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(9)
            .setName("HTTP GET /")
            .setSource("source-processtag")
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "23cace"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(4)
            .setName("HTTP GET")
            .setSource("source-processtag")
            .setSpanId("00000000-0000-0000-0000-00000012d687")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "12d687"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(3456)
            .setName("HTTP GET /test")
            .setSource("source-processtag")
            .setSpanId("00000000-0000-0000-0000-0051759bfc69")
            .setTraceId("0000011e-ab2a-9944-0000-000049631900")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "51759bfc69"),
                    new Annotation("jaegerTraceId", "11eab2a99440000000049631900"),
                    new Annotation("service", "frontend"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();
    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerTChannelCollectorHandler handler =
        new JaegerTChannelCollectorHandler(
            "9876",
            mockTraceHandler,
            mockTraceLogsHandler,
            null,
            () -> false,
            () -> false,
            null,
            new SpanSampler(new RateSampler(1.0D), () -> null),
            null,
            null);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    Tag sourceProcessTag = new Tag("source", TagType.STRING);
    sourceProcessTag.setVStr("source-processtag");

    Tag customServiceProcessTag = new Tag("service", TagType.STRING);
    customServiceProcessTag.setVStr("service-processtag");

    Tag customServiceSpanTag = new Tag("service", TagType.STRING);
    customServiceSpanTag.setVStr("service-spantag");

    io.jaegertracing.thriftjava.Span span1 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L, 1234567890L, 2345678L, 0, "HTTP GET /", 1, startTime * 1000, 9 * 1000);
    span1.setTags(ImmutableList.of(customServiceSpanTag));

    io.jaegertracing.thriftjava.Span span2 =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L, 1234567890L, 1234567L, 0, "HTTP GET", 1, startTime * 1000, 4 * 1000);

    io.jaegertracing.thriftjava.Span span3 =
        new io.jaegertracing.thriftjava.Span(
            1231231232L,
            1231232342340L,
            349865507945L,
            0,
            "HTTP GET /test",
            1,
            startTime * 1000,
            3456 * 1000);

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    testBatch.process.setTags(ImmutableList.of(ipTag, sourceProcessTag, customServiceProcessTag));

    testBatch.setSpans(ImmutableList.of(span1, span2));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batches)
            .build();
    handler.handleImpl(request);

    Batch testBatchWithoutProcessTag = new Batch();
    testBatchWithoutProcessTag.process = new Process();
    testBatchWithoutProcessTag.process.serviceName = "frontend";
    testBatchWithoutProcessTag.process.setTags(ImmutableList.of(ipTag, sourceProcessTag));
    testBatchWithoutProcessTag.setSpans(ImmutableList.of(span3));

    Collector.submitBatches_args batchesWithoutProcessTags = new Collector.submitBatches_args();
    batchesWithoutProcessTags.addToBatches(testBatchWithoutProcessTag);
    ThriftRequest<Collector.submitBatches_args> requestForProxyLevel =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batchesWithoutProcessTags)
            .build();
    handler.handleImpl(requestForProxyLevel);

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testProtectedTagsSpanOverridesProcess() throws Exception {
    // cluster, shard and service are special tags, because they're indexed by wavefront
    // The priority order is:
    // Span Level > Process Level > Proxy Level > Default
    reset(mockTraceHandler, mockTraceLogsHandler);

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(9)
            .setName("HTTP GET /")
            .setSource("source-processtag")
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "23cace"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("application", "application-spantag"),
                    new Annotation("cluster", "cluster-spantag"),
                    new Annotation("shard", "shard-spantag")))
            .build());
    expectLastCall();
    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerTChannelCollectorHandler handler =
        new JaegerTChannelCollectorHandler(
            "9876",
            mockTraceHandler,
            mockTraceLogsHandler,
            null,
            () -> false,
            () -> false,
            null,
            new SpanSampler(new RateSampler(1.0D), () -> null),
            null,
            null);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    Tag sourceProcessTag = new Tag("source", TagType.STRING);
    sourceProcessTag.setVStr("source-processtag");

    Tag customApplicationProcessTag = new Tag("application", TagType.STRING);
    customApplicationProcessTag.setVStr("application-processtag");

    Tag customApplicationSpanTag = new Tag("application", TagType.STRING);
    customApplicationSpanTag.setVStr("application-spantag");

    Tag customClusterProcessTag = new Tag("cluster", TagType.STRING);
    customClusterProcessTag.setVStr("cluster-processtag");

    Tag customClusterSpanTag = new Tag("cluster", TagType.STRING);
    customClusterSpanTag.setVStr("cluster-spantag");

    Tag customShardProcessTag = new Tag("shard", TagType.STRING);
    customShardProcessTag.setVStr("shard-processtag");

    Tag customShardSpanTag = new Tag("shard", TagType.STRING);
    customShardSpanTag.setVStr("shard-spantag");

    io.jaegertracing.thriftjava.Span span =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L, 1234567890L, 2345678L, 0, "HTTP GET /", 1, startTime * 1000, 9 * 1000);
    span.setTags(
        ImmutableList.of(customApplicationSpanTag, customClusterSpanTag, customShardSpanTag));

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    testBatch.process.setTags(
        ImmutableList.of(
            ipTag,
            sourceProcessTag,
            customApplicationProcessTag,
            customClusterProcessTag,
            customShardProcessTag));

    testBatch.setSpans(ImmutableList.of(span));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batches)
            .build();
    handler.handleImpl(request);

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testProtectedTagsProcessOverridesProxyConfig() throws Exception {
    // cluster, shard and service are special tags, because they're indexed by wavefront
    // The priority order is:
    // Span Level > Process Level > Proxy Level > Default
    reset(mockTraceHandler, mockTraceLogsHandler);

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(9)
            .setName("HTTP GET /")
            .setSource("source-processtag")
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("jaegerSpanId", "23cace"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("application", "application-processtag"),
                    new Annotation("cluster", "cluster-processtag"),
                    new Annotation("shard", "shard-processtag")))
            .build());
    expectLastCall();
    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerTChannelCollectorHandler handler =
        new JaegerTChannelCollectorHandler(
            "9876",
            mockTraceHandler,
            mockTraceLogsHandler,
            null,
            () -> false,
            () -> false,
            null,
            new SpanSampler(new RateSampler(1.0D), () -> null),
            null,
            null);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    Tag sourceProcessTag = new Tag("source", TagType.STRING);
    sourceProcessTag.setVStr("source-processtag");

    Tag customApplicationProcessTag = new Tag("application", TagType.STRING);
    customApplicationProcessTag.setVStr("application-processtag");

    Tag customClusterProcessTag = new Tag("cluster", TagType.STRING);
    customClusterProcessTag.setVStr("cluster-processtag");

    Tag customShardProcessTag = new Tag("shard", TagType.STRING);
    customShardProcessTag.setVStr("shard-processtag");

    io.jaegertracing.thriftjava.Span span =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L, 1234567890L, 2345678L, 0, "HTTP GET /", 1, startTime * 1000, 9 * 1000);

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    testBatch.process.setTags(
        ImmutableList.of(
            ipTag,
            sourceProcessTag,
            customApplicationProcessTag,
            customClusterProcessTag,
            customShardProcessTag));

    testBatch.setSpans(ImmutableList.of(span));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batches)
            .build();
    handler.handleImpl(request);

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testAllProcessTagsPropagated() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);

    mockTraceHandler.report(
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(9)
            .setName("HTTP GET /")
            .setSource("source-spantag")
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("ip", "10.0.0.1"),
                    new Annotation("processTag1", "one"),
                    new Annotation("processTag2", "two"),
                    new Annotation("processTag3", "three"),
                    new Annotation("jaegerSpanId", "23cace"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("parent", "00000000-0000-0000-0000-00000012d687"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();
    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerTChannelCollectorHandler handler =
        new JaegerTChannelCollectorHandler(
            "9876",
            mockTraceHandler,
            mockTraceLogsHandler,
            null,
            () -> false,
            () -> false,
            null,
            new SpanSampler(new RateSampler(1.0D), () -> null),
            null,
            null);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    Tag hostNameProcessTag = new Tag("hostname", TagType.STRING);
    hostNameProcessTag.setVStr("hostname-processtag");

    Tag customProcessTag1 = new Tag("processTag1", TagType.STRING);
    customProcessTag1.setVStr("one");

    Tag customProcessTag2 = new Tag("processTag2", TagType.STRING);
    customProcessTag2.setVStr("two");

    Tag customProcessTag3 = new Tag("processTag3", TagType.STRING);
    customProcessTag3.setVStr("three");

    Tag customSourceSpanTag = new Tag("source", TagType.STRING);
    customSourceSpanTag.setVStr("source-spantag");

    io.jaegertracing.thriftjava.Span span =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L,
            1234567890L,
            2345678L,
            1234567L,
            "HTTP GET /",
            1,
            startTime * 1000,
            9 * 1000);
    span.setTags(ImmutableList.of(customSourceSpanTag));

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    testBatch.process.setTags(
        ImmutableList.of(
            ipTag, hostNameProcessTag, customProcessTag1, customProcessTag2, customProcessTag3));

    testBatch.setSpans(ImmutableList.of(span));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batches)
            .build();
    handler.handleImpl(request);

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testJaegerSamplerSync() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);

    Span expectedSpan =
        Span.newBuilder()
            .setCustomer("dummy")
            .setStartMillis(startTime)
            .setDuration(9)
            .setName("HTTP GET /")
            .setSource(DEFAULT_SOURCE)
            .setSpanId("00000000-0000-0000-0000-00000023cace")
            .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(
                ImmutableList.of(
                    new Annotation("jaegerSpanId", "23cace"),
                    new Annotation("jaegerTraceId", "499602d20000011f71fb04cb"),
                    new Annotation("service", "frontend"),
                    new Annotation("parent", "00000000-0000-0000-0000-00000012d687"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none"),
                    new Annotation("_spanLogs", "true")))
            .build();
    mockTraceHandler.report(expectedSpan);
    expectLastCall();

    Capture<SpanLogs> spanLogsCapture = newCapture();
    mockTraceLogsHandler.report(capture(spanLogsCapture));
    expectLastCall();

    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerTChannelCollectorHandler handler =
        new JaegerTChannelCollectorHandler(
            "9876",
            mockTraceHandler,
            mockTraceLogsHandler,
            null,
            () -> false,
            () -> false,
            null,
            new SpanSampler(new DurationSampler(5), () -> null),
            null,
            null);

    io.jaegertracing.thriftjava.Span span =
        new io.jaegertracing.thriftjava.Span(
            1234567890123L,
            1234567890L,
            2345678L,
            1234567L,
            "HTTP GET /",
            1,
            startTime * 1000,
            9 * 1000);

    Tag tag = new Tag("event", TagType.STRING);
    tag.setVStr("error");

    span.setLogs(ImmutableList.of(new Log(startTime * 1000, ImmutableList.of(tag))));

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";

    testBatch.setSpans(ImmutableList.of(span));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request =
        new ThriftRequest.Builder<Collector.submitBatches_args>(
                "jaeger-collector", "Collector::submitBatches")
            .setBody(batches)
            .build();
    handler.handleImpl(request);

    assertEquals("_sampledByPolicy=NONE", spanLogsCapture.getValue().getSpan());
    verify(mockTraceHandler, mockTraceLogsHandler);
  }
}
