package com.wavefront.agent.listeners.tracing;

import com.google.common.collect.ImmutableList;

import com.uber.tchannel.messages.ThriftRequest;
import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.sdk.entities.tracing.sampling.RateSampler;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import io.jaegertracing.thriftjava.Batch;
import io.jaegertracing.thriftjava.Collector;
import io.jaegertracing.thriftjava.Process;
import io.jaegertracing.thriftjava.Tag;
import io.jaegertracing.thriftjava.TagType;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

public class JaegerThriftCollectorHandlerTest {
  private ReportableEntityHandler<Span> mockTraceHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private ReportableEntityHandler<SpanLogs> mockTraceLogsHandler =
      MockReportableEntityHandlerFactory.getMockTraceSpanLogsHandler();
  private long startTime = System.currentTimeMillis();

  @Test
  public void testJaegerThriftCollector() throws Exception {
    reset(mockTraceHandler);
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(1234)
        .setName("HTTP GET")
        .setSource("10.0.0.1")
        .setSpanId("00000000-0000-0000-0000-00000012d687")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("service", "frontend"),
            new Annotation("component", "db"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(2345)
        .setName("HTTP GET /")
        .setSource("10.0.0.1")
        .setSpanId("00000000-0000-0000-0000-00000023cace")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("service", "frontend"),
            new Annotation("parent", "00000000-0000-0000-0000-00000012d687"),
            new Annotation("component", "db"),
            new Annotation("application", "Custom-JaegerApp"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(3456)
        .setName("HTTP GET /")
        .setSource("10.0.0.1")
        .setSpanId("00000000-0000-0000-9a12-b85901d53397")
        .setTraceId("00000000-0000-0000-fea4-87ee36e58cab")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("service", "frontend"),
            new Annotation("parent", "00000000-0000-0000-fea4-87ee36e58cab"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();

    // Test filtering empty tags
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
            .setDuration(3456)
            .setName("HTTP GET /test")
            .setSource("10.0.0.1")
            .setSpanId("00000000-0000-0000-0000-0051759bfc69")
            .setTraceId("0000011e-ab2a-9944-0000-000049631900")
            // Note: Order of annotations list matters for this unit test.
            .setAnnotations(ImmutableList.of(
                    new Annotation("service", "frontend"),
                    new Annotation("application", "Jaeger"),
                    new Annotation("cluster", "none"),
                    new Annotation("shard", "none")))
            .build());
    expectLastCall();

    replay(mockTraceHandler);

    JaegerThriftCollectorHandler handler = new JaegerThriftCollectorHandler("9876", mockTraceHandler,
        mockTraceLogsHandler, null, new AtomicBoolean(false), null, new RateSampler(1.0D), false,
        null, null);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    Tag componentTag = new Tag("component", TagType.STRING);
    componentTag.setVStr("db");

    Tag customApplicationTag = new Tag("application", TagType.STRING);
    customApplicationTag.setVStr("Custom-JaegerApp");

    Tag emptyTag = new Tag("empty", TagType.STRING);
    emptyTag.setVStr("");

    io.jaegertracing.thriftjava.Span span1 = new io.jaegertracing.thriftjava.Span(1234567890123L, 1234567890L,
        1234567L, 0L, "HTTP GET", 1, startTime * 1000, 1234 * 1000);

    io.jaegertracing.thriftjava.Span span2 = new io.jaegertracing.thriftjava.Span(1234567890123L, 1234567890L,
        2345678L, 1234567L, "HTTP GET /", 1, startTime * 1000, 2345 * 1000);

    // check negative span IDs too
    io.jaegertracing.thriftjava.Span span3 = new io.jaegertracing.thriftjava.Span(-97803834702328661L, 0L,
        -7344605349865507945L, -97803834702328661L, "HTTP GET /", 1, startTime * 1000, 3456 * 1000);

    io.jaegertracing.thriftjava.Span span4 = new io.jaegertracing.thriftjava.Span(1231231232L, 1231232342340L,
            349865507945L, 0, "HTTP GET /test", 1, startTime * 1000, 3456 * 1000);

    span1.setTags(ImmutableList.of(componentTag));
    span2.setTags(ImmutableList.of(componentTag, customApplicationTag));
    span4.setTags(ImmutableList.of(emptyTag));

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    testBatch.process.setTags(ImmutableList.of(ipTag));

    testBatch.setSpans(ImmutableList.of(span1, span2, span3, span4));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request = new ThriftRequest.Builder<Collector.submitBatches_args>(
        "jaeger-collector", "Collector::submitBatches").setBody(batches).build();
    handler.handleImpl(request);

    verify(mockTraceHandler);
  }

  @Test
  public void testApplicationTagPriority() throws Exception {
    reset(mockTraceHandler);

    // Span to verify span level tags precedence
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(1234)
        .setName("HTTP GET")
        .setSource("10.0.0.1")
        .setSpanId("00000000-0000-0000-0000-00000012d687")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("service", "frontend"),
            new Annotation("component", "db"),
            new Annotation("application", "SpanLevelAppTag"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();

    // Span to verify process level tags precedence
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(2345)
        .setName("HTTP GET /")
        .setSource("10.0.0.1")
        .setSpanId("00000000-0000-0000-0000-00000023cace")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("service", "frontend"),
            new Annotation("parent", "00000000-0000-0000-0000-00000012d687"),
            new Annotation("component", "db"),
            new Annotation("application", "ProcessLevelAppTag"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();

    // Span to verify Proxy level tags precedence
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(3456)
        .setName("HTTP GET /")
        .setSource("10.0.0.1")
        .setSpanId("00000000-0000-0000-9a12-b85901d53397")
        .setTraceId("00000000-0000-0000-fea4-87ee36e58cab")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("service", "frontend"),
            new Annotation("parent", "00000000-0000-0000-fea4-87ee36e58cab"),
            new Annotation("application", "ProxyLevelAppTag"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();
    replay(mockTraceHandler);

    // Verify span level "application" tags precedence
    JaegerThriftCollectorHandler handler = new JaegerThriftCollectorHandler("9876", mockTraceHandler,
        mockTraceLogsHandler, null, new AtomicBoolean(false), null, new RateSampler(1.0D), false,
        "ProxyLevelAppTag", null);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    Tag componentTag = new Tag("component", TagType.STRING);
    componentTag.setVStr("db");

    Tag spanLevelAppTag = new Tag("application", TagType.STRING);
    spanLevelAppTag.setVStr("SpanLevelAppTag");

    Tag processLevelAppTag = new Tag("application", TagType.STRING);
    processLevelAppTag.setVStr("ProcessLevelAppTag");

    io.jaegertracing.thriftjava.Span span1 = new io.jaegertracing.thriftjava.Span(1234567890123L, 1234567890L,
        1234567L, 0L, "HTTP GET", 1, startTime * 1000, 1234 * 1000);

    io.jaegertracing.thriftjava.Span span2 = new io.jaegertracing.thriftjava.Span(1234567890123L, 1234567890L,
        2345678L, 1234567L, "HTTP GET /", 1, startTime * 1000, 2345 * 1000);

    // check negative span IDs too
    io.jaegertracing.thriftjava.Span span3 = new io.jaegertracing.thriftjava.Span(-97803834702328661L, 0L,
        -7344605349865507945L, -97803834702328661L, "HTTP GET /", 1, startTime * 1000, 3456 * 1000);

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
    ThriftRequest<Collector.submitBatches_args> request = new ThriftRequest.Builder<Collector.submitBatches_args>(
        "jaeger-collector", "Collector::submitBatches").setBody(batches).build();
    handler.handleImpl(request);

    // Span3 to verify process level tags precedence. So do not set any process level tag.
    Batch testBatchForProxyLevel = new Batch();
    testBatchForProxyLevel.process = new Process();
    testBatchForProxyLevel.process.serviceName = "frontend";
    testBatchForProxyLevel.process.setTags(ImmutableList.of(ipTag));

    testBatchForProxyLevel.setSpans(ImmutableList.of(span3));

    Collector.submitBatches_args batchesForProxyLevel = new Collector.submitBatches_args();
    batchesForProxyLevel.addToBatches(testBatchForProxyLevel);
    ThriftRequest<Collector.submitBatches_args> requestForProxyLevel = new ThriftRequest.
        Builder<Collector.submitBatches_args>("jaeger-collector", "Collector::submitBatches").
        setBody(batchesForProxyLevel).
        build();
    handler.handleImpl(requestForProxyLevel);

    verify(mockTraceHandler);
  }
}
