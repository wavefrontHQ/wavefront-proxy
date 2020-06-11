package com.wavefront.agent.listeners.tracing;

import com.google.common.collect.ImmutableList;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;

import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.sdk.entities.tracing.sampling.DurationSampler;
import com.wavefront.sdk.entities.tracing.sampling.RateSampler;

import org.junit.Test;

import java.nio.ByteBuffer;

import io.grpc.stub.StreamObserver;

import io.opentelemetry.exporters.jaeger.proto.api_v2.Collector;
import io.opentelemetry.exporters.jaeger.proto.api_v2.Model;

import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

/**
 * Unit tests for {@link JaegerGrpcCollectorHandler}
 *
 * @author Hao Song (songhao@vmware.com)
 */
public class JaegerGrpcCollectorHandlerTest {
  private final static String DEFAULT_SOURCE = "jaeger";
  private ReportableEntityHandler<Span, String> mockTraceHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private ReportableEntityHandler<SpanLogs, String> mockTraceLogsHandler =
      MockReportableEntityHandlerFactory.getMockTraceSpanLogsHandler();
  private long startTime = System.currentTimeMillis();

  @Test
  public void testJaegerGrpcCollector() throws Exception {

    reset(mockTraceHandler, mockTraceLogsHandler);
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").
        setStartMillis(startTime)
        .setDuration(1000)
        .setName("HTTP GET")
        .setSource(DEFAULT_SOURCE)
        .setSpanId("00000000-0000-0000-0000-00000012d687")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("component", "db"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("_spanLogs", "true")))
        .build());
    expectLastCall();

    mockTraceLogsHandler.report(SpanLogs.newBuilder().
        setCustomer("default").
        setSpanId("00000000-0000-0000-0000-00000012d687").
        setTraceId("00000000-4996-02d2-0000-011f71fb04cb").
        setLogs(ImmutableList.of(
            SpanLog.newBuilder().
                setTimestamp(startTime * 1000).
                setFields(ImmutableMap.of("event", "error", "exception", "NullPointerException")).
                build()
        )).
        build());
    expectLastCall();

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(2000)
        .setName("HTTP GET /")
        .setSource(DEFAULT_SOURCE)
        .setSpanId("00000000-0000-0000-0000-00000023cace")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("component", "db"),
            new Annotation("application", "Custom-JaegerApp"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("parent", "00000000-0000-0000-0000-00000012d687")))
        .build());
    expectLastCall();

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(2000)
        .setName("HTTP GET /")
        .setSource(DEFAULT_SOURCE)
        .setSpanId("00000000-0000-0000-9a12-b85901d53397")
        .setTraceId("00000000-0000-0000-fea4-87ee36e58cab")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("parent", "00000000-0000-0000-fea4-87ee36e58cab")))
        .build());
    expectLastCall();

    // Test filtering empty tags
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(2000)
        .setName("HTTP GET /test")
        .setSource(DEFAULT_SOURCE)
        .setSpanId("00000000-0000-0000-0000-0051759bfc69")
        .setTraceId("0000011e-ab2a-9944-0000-000049631900")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();

    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerGrpcCollectorHandler handler = new JaegerGrpcCollectorHandler("9876", mockTraceHandler,
        mockTraceLogsHandler, null, () -> false, () -> false, null,
        new SpanSampler(new RateSampler(1.0D), false),
        null, null);

    Model.KeyValue ipTag = Model.KeyValue.newBuilder().
        setKey("ip").
        setVStr("10.0.0.1").
        setVType(Model.ValueType.STRING).
        build();

    Model.KeyValue componentTag = Model.KeyValue.newBuilder().
        setKey("component").
        setVStr("db").
        setVType(Model.ValueType.STRING).
        build();

    Model.KeyValue customApplicationTag = Model.KeyValue.newBuilder().
        setKey("application").
        setVStr("Custom-JaegerApp").
        setVType(Model.ValueType.STRING).
        build();

    Model.KeyValue emptyTag = Model.KeyValue.newBuilder().
        setKey("empty").
        setVStr("").
        setVType(Model.ValueType.STRING).
        build();

    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
    buffer.putLong(1234567890L);
    buffer.putLong(1234567890123L);
    ByteString traceId = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES * 2);
    buffer.putLong(0L);
    buffer.putLong(-97803834702328661L);
    ByteString trace3Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES * 2);
    buffer.putLong(1231232342340L);
    buffer.putLong(1231231232L);
    ByteString trace4Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(1234567L);
    ByteString span1Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(2345678L);
    ByteString span2Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(-7344605349865507945L);
    ByteString span3Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(-97803834702328661L);
    ByteString span3ParentId = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(349865507945L);
    ByteString span4Id = ByteString.copyFrom(buffer.array());

    Model.Span span1 = Model.Span.newBuilder().
        setTraceId(traceId).
        setSpanId(span1Id).
        setDuration(Duration.newBuilder().setSeconds(1L).build()).
        setOperationName("HTTP GET").
        setStartTime(fromMillis(startTime)).
        addTags(componentTag).
        addLogs(Model.Log.newBuilder().
            addFields(Model.KeyValue.newBuilder().setKey("event").setVStr("error").setVType(Model.ValueType.STRING).build()).
            addFields(Model.KeyValue.newBuilder().setKey("exception").setVStr("NullPointerException").setVType(Model.ValueType.STRING).build()).
            setTimestamp(fromMillis(startTime))).
        build();

    Model.Span span2 = Model.Span.newBuilder().
        setTraceId(traceId).
        setSpanId(span2Id).
        setDuration(Duration.newBuilder().setSeconds(2L).build()).
        setOperationName("HTTP GET /").
        setStartTime(fromMillis(startTime)).
        addTags(componentTag).
        addTags(customApplicationTag).
        addReferences(Model.SpanRef.newBuilder().setRefType(Model.SpanRefType.CHILD_OF).setSpanId(span1Id).setTraceId(traceId).build()).
        build();

    // check negative span IDs too
    Model.Span span3 = Model.Span.newBuilder().
        setTraceId(trace3Id).
        setSpanId(span3Id).
        setDuration(Duration.newBuilder().setSeconds(2L).build()).
        setOperationName("HTTP GET /").
        setStartTime(fromMillis(startTime)).
        addReferences(Model.SpanRef.newBuilder().setRefType(Model.SpanRefType.CHILD_OF).setSpanId(span3ParentId).setTraceId(traceId).build()).
        build();

    Model.Span span4 = Model.Span.newBuilder().
        setTraceId(trace4Id).
        setSpanId(span4Id).
        setDuration(Duration.newBuilder().setSeconds(2L).build()).
        setOperationName("HTTP GET /test").
        setStartTime(fromMillis(startTime)).
        addTags(emptyTag).
        build();

    Model.Batch testBatch = Model.Batch.newBuilder().
        setProcess(Model.Process.newBuilder().setServiceName("frontend").addTags(ipTag).build()).
        addAllSpans(ImmutableList.of(span1, span2, span3, span4)).
        build();

    Collector.PostSpansRequest batches =
        Collector.PostSpansRequest.newBuilder().setBatch(testBatch).build();

    handler.postSpans(batches, new StreamObserver<Collector.PostSpansResponse>() {
      @Override
      public void onNext(Collector.PostSpansResponse postSpansResponse) {
      }

      @Override
      public void onError(Throwable throwable) {
      }

      @Override
      public void onCompleted() {
      }
    });

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testApplicationTagPriority() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);

    // Span to verify span level tags precedence
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(1000)
        .setName("HTTP GET")
        .setSource(DEFAULT_SOURCE)
        .setSpanId("00000000-0000-0000-0000-00000012d687")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("component", "db"),
            new Annotation("application", "SpanLevelAppTag"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();

    // Span to verify process level tags precedence
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(2000)
        .setName("HTTP GET /")
        .setSource(DEFAULT_SOURCE)
        .setSpanId("00000000-0000-0000-0000-00000023cace")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("component", "db"),
            new Annotation("application", "ProcessLevelAppTag"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("parent", "00000000-0000-0000-0000-00000012d687")))
        .build());
    expectLastCall();

    // Span to verify Proxy level tags precedence
    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(3000)
        .setName("HTTP GET /")
        .setSource(DEFAULT_SOURCE)
        .setSpanId("00000000-0000-0000-9a12-b85901d53397")
        .setTraceId("00000000-0000-0000-fea4-87ee36e58cab")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("application", "ProxyLevelAppTag"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("parent", "00000000-0000-0000-fea4-87ee36e58cab")))
        .build());
    expectLastCall();
    replay(mockTraceHandler, mockTraceLogsHandler);

    // Verify span level "application" tags precedence
    JaegerGrpcCollectorHandler handler = new JaegerGrpcCollectorHandler("9876", mockTraceHandler,
        mockTraceLogsHandler, null, () -> false, () -> false, null, new SpanSampler(new RateSampler(1.0D),
        false), "ProxyLevelAppTag", null);

    Model.KeyValue ipTag = Model.KeyValue.newBuilder().
        setKey("ip").
        setVStr("10.0.0.1").
        setVType(Model.ValueType.STRING).
        build();

    Model.KeyValue componentTag = Model.KeyValue.newBuilder().
        setKey("component").
        setVStr("db").
        setVType(Model.ValueType.STRING).
        build();

    Model.KeyValue spanLevelAppTag = Model.KeyValue.newBuilder().
        setKey("application").
        setVStr("SpanLevelAppTag").
        setVType(Model.ValueType.STRING).
        build();

    Model.KeyValue processLevelAppTag = Model.KeyValue.newBuilder().
        setKey("application").
        setVStr("ProcessLevelAppTag").
        setVType(Model.ValueType.STRING).
        build();

    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
    buffer.putLong(1234567890L);
    buffer.putLong(1234567890123L);
    ByteString traceId = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES * 2);
    buffer.putLong(0L);
    buffer.putLong(-97803834702328661L);
    ByteString trace2Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(1234567L);
    ByteString span1Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(2345678L);
    ByteString span2Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(-7344605349865507945L);
    ByteString span3Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(-97803834702328661L);
    ByteString span3ParentId = ByteString.copyFrom(buffer.array());

    // Span1 to verify span level tags precedence
    Model.Span span1 = Model.Span.newBuilder().
        setTraceId(traceId).
        setSpanId(span1Id).
        setDuration(Duration.newBuilder().setSeconds(1L).build()).
        setOperationName("HTTP GET").
        setStartTime(fromMillis(startTime)).
        addTags(componentTag).
        addTags(spanLevelAppTag).
        setFlags(1).
        build();

    Model.Span span2 = Model.Span.newBuilder().
        setTraceId(traceId).
        setSpanId(span2Id).
        setDuration(Duration.newBuilder().setSeconds(2L).build()).
        setOperationName("HTTP GET /").
        setStartTime(fromMillis(startTime)).
        addTags(componentTag).
        setFlags(1).
        addReferences(Model.SpanRef.newBuilder().setRefType(Model.SpanRefType.CHILD_OF).setSpanId(span1Id).setTraceId(traceId).build()).
        build();

    // check negative span IDs too
    Model.Span span3 = Model.Span.newBuilder().
        setTraceId(trace2Id).
        setSpanId(span3Id).
        setDuration(Duration.newBuilder().setSeconds(3L).build()).
        setOperationName("HTTP GET /").
        setStartTime(fromMillis(startTime)).
        setFlags(1).
        addReferences(Model.SpanRef.newBuilder().setRefType(Model.SpanRefType.CHILD_OF).setSpanId(span3ParentId).setTraceId(traceId).build()).
        build();

    StreamObserver<Collector.PostSpansResponse> streamObserver = new StreamObserver<Collector.PostSpansResponse>() {
      @Override
      public void onNext(Collector.PostSpansResponse postSpansResponse) {
      }

      @Override
      public void onError(Throwable throwable) {
      }

      @Override
      public void onCompleted() {
      }
    };

    Model.Batch testBatch = Model.Batch.newBuilder().
        setProcess(Model.Process.newBuilder().setServiceName("frontend").addTags(ipTag).addTags(processLevelAppTag).build()).
        addAllSpans(ImmutableList.of(span1, span2)).
        build();

    Collector.PostSpansRequest batches =
        Collector.PostSpansRequest.newBuilder().setBatch(testBatch).build();

    handler.postSpans(batches, streamObserver);

    Model.Batch testBatchForProxyLevel = Model.Batch.newBuilder().
        setProcess(Model.Process.newBuilder().setServiceName("frontend").addTags(ipTag).build()).
        addAllSpans(ImmutableList.of(span3)).
        build();

    Collector.PostSpansRequest batchesForProxyLevel =
        Collector.PostSpansRequest.newBuilder().setBatch(testBatchForProxyLevel).build();

    handler.postSpans(batchesForProxyLevel, streamObserver);

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testJaegerDurationSampler() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(9000)
        .setName("HTTP GET /")
        .setSource(DEFAULT_SOURCE)
        .setSpanId("00000000-0000-0000-0000-00000023cace")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("parent", "00000000-0000-0000-0000-00000012d687")))
        .build());
    expectLastCall();

    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerGrpcCollectorHandler handler = new JaegerGrpcCollectorHandler("9876", mockTraceHandler,
        mockTraceLogsHandler, null, () -> false, () -> false, null,
        new SpanSampler(new DurationSampler(5 * 1000), false), null, null);

    Model.KeyValue ipTag = Model.KeyValue.newBuilder().
        setKey("ip").
        setVStr("10.0.0.1").
        setVType(Model.ValueType.STRING).
        build();

    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
    buffer.putLong(1234567890L);
    buffer.putLong(1234567890123L);
    ByteString traceId = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(1234567L);
    ByteString span1Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(2345678L);
    ByteString span2Id = ByteString.copyFrom(buffer.array());

    Model.Span span1 = Model.Span.newBuilder().
        setTraceId(traceId).
        setSpanId(span1Id).
        setDuration(Duration.newBuilder().setSeconds(4L).build()).
        setOperationName("HTTP GET").
        setStartTime(fromMillis(startTime)).
        build();

    Model.Span span2 = Model.Span.newBuilder().
        setTraceId(traceId).
        setSpanId(span2Id).
        setDuration(Duration.newBuilder().setSeconds(9L).build()).
        setOperationName("HTTP GET /").
        setStartTime(fromMillis(startTime)).
        addReferences(Model.SpanRef.newBuilder().setRefType(Model.SpanRefType.CHILD_OF).setSpanId(span1Id).setTraceId(traceId).build()).
        build();

    Model.Batch testBatch = Model.Batch.newBuilder().
        setProcess(Model.Process.newBuilder().setServiceName("frontend").addTags(ipTag).build()).
        addAllSpans(ImmutableList.of(span1, span2)).
        build();

    Collector.PostSpansRequest batches =
        Collector.PostSpansRequest.newBuilder().setBatch(testBatch).build();

    handler.postSpans(batches, new StreamObserver<Collector.PostSpansResponse>() {
      @Override
      public void onNext(Collector.PostSpansResponse postSpansResponse) {
      }

      @Override
      public void onError(Throwable throwable) {
      }

      @Override
      public void onCompleted() {
      }
    });

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testJaegerDebugOverride() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(9000)
        .setName("HTTP GET /")
        .setSource(DEFAULT_SOURCE)
        .setSpanId("00000000-0000-0000-0000-00000023cace")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("debug", "true1"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("parent", "00000000-0000-0000-0000-00000012d687")))
        .build());
    expectLastCall();

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(4000)
        .setName("HTTP GET")
        .setSource(DEFAULT_SOURCE)
        .setSpanId("00000000-0000-0000-0000-00000012d687")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("sampling.priority", "0.3"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();

    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerGrpcCollectorHandler handler = new JaegerGrpcCollectorHandler("9876", mockTraceHandler,
        mockTraceLogsHandler, null, () -> false, () -> false, null,
        new SpanSampler(new DurationSampler(10 * 1000), false), null, null);

    Model.KeyValue ipTag = Model.KeyValue.newBuilder().
        setKey("ip").
        setVStr("10.0.0.1").
        setVType(Model.ValueType.STRING).
        build();

    Model.KeyValue debugTag = Model.KeyValue.newBuilder().
        setKey("debug").
        setVStr("true1").
        setVType(Model.ValueType.STRING).
        build();

    Model.KeyValue samplePriorityTag = Model.KeyValue.newBuilder().
        setKey("sampling.priority").
        setVFloat64(0.3).
        setVType(Model.ValueType.FLOAT64).
        build();

    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
    buffer.putLong(1234567890L);
    buffer.putLong(1234567890123L);
    ByteString traceId = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(2345678L);
    ByteString span1Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(1234567L);
    ByteString span2Id = ByteString.copyFrom(buffer.array());

    Model.Span span1 = Model.Span.newBuilder().
        setTraceId(traceId).
        setSpanId(span1Id).
        setDuration(Duration.newBuilder().setSeconds(9L).build()).
        setOperationName("HTTP GET /").
        addTags(debugTag).
        addReferences(Model.SpanRef.newBuilder().setRefType(Model.SpanRefType.CHILD_OF).setSpanId(span2Id).setTraceId(traceId).build()).
        setStartTime(fromMillis(startTime)).
        build();

    Model.Span span2 = Model.Span.newBuilder().
        setTraceId(traceId).
        setSpanId(span2Id).
        setDuration(Duration.newBuilder().setSeconds(4L).build()).
        setOperationName("HTTP GET").
        addTags(samplePriorityTag).
        setStartTime(fromMillis(startTime)).
        build();

    Model.Batch testBatch = Model.Batch.newBuilder().
        setProcess(Model.Process.newBuilder().setServiceName("frontend").addTags(ipTag).build()).
        addAllSpans(ImmutableList.of(span1, span2)).
        build();

    Collector.PostSpansRequest batches =
        Collector.PostSpansRequest.newBuilder().setBatch(testBatch).build();

    handler.postSpans(batches, new StreamObserver<Collector.PostSpansResponse>() {
      @Override
      public void onNext(Collector.PostSpansResponse postSpansResponse) {
      }

      @Override
      public void onError(Throwable throwable) {
      }

      @Override
      public void onCompleted() {
      }
    });

    verify(mockTraceHandler, mockTraceLogsHandler);
  }

  @Test
  public void testSourceTagPriority() throws Exception {
    reset(mockTraceHandler, mockTraceLogsHandler);

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(9000)
        .setName("HTTP GET /")
        .setSource("source-spantag")
        .setSpanId("00000000-0000-0000-0000-00000023cace")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none"),
            new Annotation("parent", "00000000-0000-0000-0000-00000012d687")))
        .build());
    expectLastCall();

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(4000)
        .setName("HTTP GET")
        .setSource("source-processtag")
        .setSpanId("00000000-0000-0000-0000-00000012d687")
        .setTraceId("00000000-4996-02d2-0000-011f71fb04cb")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();

    mockTraceHandler.report(Span.newBuilder().setCustomer("dummy").setStartMillis(startTime)
        .setDuration(3000)
        .setName("HTTP GET /test")
        .setSource("hostname-processtag")
        .setSpanId("00000000-0000-0000-0000-0051759bfc69")
        .setTraceId("0000011e-ab2a-9944-0000-000049631900")
        // Note: Order of annotations list matters for this unit test.
        .setAnnotations(ImmutableList.of(
            new Annotation("ip", "10.0.0.1"),
            new Annotation("service", "frontend"),
            new Annotation("application", "Jaeger"),
            new Annotation("cluster", "none"),
            new Annotation("shard", "none")))
        .build());
    expectLastCall();
    replay(mockTraceHandler, mockTraceLogsHandler);

    JaegerGrpcCollectorHandler handler = new JaegerGrpcCollectorHandler("9876",
        mockTraceHandler, mockTraceLogsHandler, null, () -> false, () -> false, null,
        new SpanSampler(new RateSampler(1.0D), false),
        null, null);

    Model.KeyValue ipTag = Model.KeyValue.newBuilder().
        setKey("ip").
        setVStr("10.0.0.1").
        setVType(Model.ValueType.STRING).
        build();

    Model.KeyValue hostNameProcessTag = Model.KeyValue.newBuilder().
        setKey("hostname").
        setVStr("hostname-processtag").
        setVType(Model.ValueType.STRING).
        build();

    Model.KeyValue customSourceProcessTag = Model.KeyValue.newBuilder().
        setKey("source").
        setVStr("source-processtag").
        setVType(Model.ValueType.STRING).
        build();

    Model.KeyValue customSourceSpanTag = Model.KeyValue.newBuilder().
        setKey("source").
        setVStr("source-spantag").
        setVType(Model.ValueType.STRING).
        build();

    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
    buffer.putLong(1234567890L);
    buffer.putLong(1234567890123L);
    ByteString traceId = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES * 2);
    buffer.putLong(1231232342340L);
    buffer.putLong(1231231232L);
    ByteString trace2Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(2345678L);
    ByteString span1Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(1234567L);
    ByteString span2Id = ByteString.copyFrom(buffer.array());

    buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(349865507945L);
    ByteString span3Id = ByteString.copyFrom(buffer.array());

    Model.Span span1 = Model.Span.newBuilder().
        setTraceId(traceId).
        setSpanId(span1Id).
        setDuration(Duration.newBuilder().setSeconds(9L).build()).
        setOperationName("HTTP GET /").
        addTags(customSourceSpanTag).
        addReferences(Model.SpanRef.newBuilder().setRefType(Model.SpanRefType.CHILD_OF).setSpanId(span2Id).setTraceId(traceId).build()).
        setStartTime(fromMillis(startTime)).
        build();

    Model.Span span2 = Model.Span.newBuilder().
        setTraceId(traceId).
        setSpanId(span2Id).
        setDuration(Duration.newBuilder().setSeconds(4L).build()).
        setOperationName("HTTP GET").
        setStartTime(fromMillis(startTime)).
        build();

    Model.Span span3 = Model.Span.newBuilder().
        setTraceId(trace2Id).
        setSpanId(span3Id).
        setDuration(Duration.newBuilder().setSeconds(3L).build()).
        setOperationName("HTTP GET /test").
        setStartTime(fromMillis(startTime)).
        build();

    StreamObserver<Collector.PostSpansResponse> streamObserver = new StreamObserver<Collector.PostSpansResponse>() {
      @Override
      public void onNext(Collector.PostSpansResponse postSpansResponse) {
      }

      @Override
      public void onError(Throwable throwable) {
      }

      @Override
      public void onCompleted() {
      }
    };

    Model.Batch testBatch = Model.Batch.newBuilder().
        setProcess(Model.Process.newBuilder().setServiceName("frontend").addTags(ipTag).addTags(hostNameProcessTag).addTags(customSourceProcessTag).build()).
        addAllSpans(ImmutableList.of(span1, span2)).
        build();

    Collector.PostSpansRequest batches =
        Collector.PostSpansRequest.newBuilder().setBatch(testBatch).build();

    handler.postSpans(batches, streamObserver);

    Model.Batch testBatchForProxyLevel = Model.Batch.newBuilder().
        setProcess(Model.Process.newBuilder().setServiceName("frontend").addTags(ipTag).addTags(hostNameProcessTag).build()).
        addAllSpans(ImmutableList.of(span3)).
        build();

    Collector.PostSpansRequest batchesSourceAsProcessTagHostName =
        Collector.PostSpansRequest.newBuilder().setBatch(testBatchForProxyLevel).build();

    handler.postSpans(batchesSourceAsProcessTagHostName, streamObserver);

    verify(mockTraceHandler, mockTraceLogsHandler);
  }
}
