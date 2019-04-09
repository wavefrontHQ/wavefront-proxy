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
            new Annotation("application", "Jaeger"),
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
            new Annotation("component", "db"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();


    replay(mockTraceHandler);

    JaegerThriftCollectorHandler handler = new JaegerThriftCollectorHandler("9876", mockTraceHandler,
        mockTraceLogsHandler, null, new AtomicBoolean(false), null, new RateSampler(1.0D), false);

    Tag ipTag = new Tag("ip", TagType.STRING);
    ipTag.setVStr("10.0.0.1");

    Tag componentTag = new Tag("component", TagType.STRING);
    componentTag.setVStr("db");

    io.jaegertracing.thriftjava.Span span1 = new io.jaegertracing.thriftjava.Span(1234567890123L, 1234567890L,
        1234567L, 0L, "HTTP GET", 1, startTime * 1000, 1234 * 1000);

    io.jaegertracing.thriftjava.Span span2 = new io.jaegertracing.thriftjava.Span(1234567890123L, 1234567890L,
        2345678L, 1234567L, "HTTP GET /", 1, startTime * 1000, 2345 * 1000);

    // check negative span IDs too
    io.jaegertracing.thriftjava.Span span3 = new io.jaegertracing.thriftjava.Span(-97803834702328661L, 0L,
        -7344605349865507945L, -97803834702328661L, "HTTP GET /", 1, startTime * 1000, 3456 * 1000);

    span1.setTags(ImmutableList.of(componentTag));
    span2.setTags(ImmutableList.of(componentTag));
    span3.setTags(ImmutableList.of(componentTag));

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    testBatch.process.setTags(ImmutableList.of(ipTag));

    testBatch.setSpans(ImmutableList.of(span1, span2, span3));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request = new ThriftRequest.Builder<Collector.submitBatches_args>(
        "jaeger-collector", "Collector::submitBatches").setBody(batches).build();
    handler.handleImpl(request);

    verify(mockTraceHandler);
  }
}
