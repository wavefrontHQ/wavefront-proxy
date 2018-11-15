package com.wavefront.agent.listeners;

import com.google.common.collect.ImmutableList;

import com.uber.tchannel.messages.ThriftRequest;
import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import io.jaegertracing.thriftjava.Batch;
import io.jaegertracing.thriftjava.Collector;
import io.jaegertracing.thriftjava.Process;
import io.jaegertracing.thriftjava.Tag;
import io.jaegertracing.thriftjava.TagType;
import wavefront.report.Annotation;
import wavefront.report.Span;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

public class JaegerThriftCollectorHandlerTest {
  private ReportableEntityHandler<Span> mockTraceHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
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
        .setAnnotations(ImmutableList.of(new Annotation("service", "frontend"),
            new Annotation("application", "Jaeger")))
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
            new Annotation("application", "Jaeger")))
        .build());
    expectLastCall();

    replay(mockTraceHandler);

    JaegerThriftCollectorHandler handler = new JaegerThriftCollectorHandler("9876", mockTraceHandler,
        new AtomicBoolean(false));

    Tag tag1 = new Tag("ip", TagType.STRING);
    tag1.setVStr("10.0.0.1");

    io.jaegertracing.thriftjava.Span span1 = new io.jaegertracing.thriftjava.Span(1234567890123L, 1234567890L,
        1234567L, 0L, "HTTP GET", 1, startTime * 1000, 1234 * 1000);

    io.jaegertracing.thriftjava.Span span2 = new io.jaegertracing.thriftjava.Span(1234567890123L, 1234567890L,
        2345678L, 1234567L, "HTTP GET /", 1, startTime * 1000, 2345 * 1000);

    Batch testBatch = new Batch();
    testBatch.process = new Process();
    testBatch.process.serviceName = "frontend";
    testBatch.process.setTags(ImmutableList.of(tag1));

    testBatch.setSpans(ImmutableList.of(span1, span2));

    Collector.submitBatches_args batches = new Collector.submitBatches_args();
    batches.addToBatches(testBatch);
    ThriftRequest<Collector.submitBatches_args> request = new ThriftRequest.Builder<Collector.submitBatches_args>(
        "jaeger-collector", "Collector::submitBatches").setBody(batches).build();
    handler.handleImpl(request);

    verify(mockTraceHandler);
  }
}
