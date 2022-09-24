package com.wavefront.agent.listeners.tracing;

import static com.wavefront.agent.listeners.tracing.SpanUtils.*;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.LineBasedAllowFilter;
import com.wavefront.agent.preprocessor.LineBasedBlockFilter;
import com.wavefront.agent.preprocessor.PreprocessorRuleMetrics;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.preprocessor.SpanBlockFilter;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.ingester.ReportableEntityDecoder;
import com.wavefront.ingester.SpanDecoder;
import com.wavefront.ingester.SpanLogsDecoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLog;
import wavefront.report.SpanLogs;

/**
 * Unit tests for {@link SpanUtils}.
 *
 * @author Shipeng Xie (xshipeng@vmware.com)
 */
public class SpanUtilsTest {
  private ReportableEntityDecoder<String, Span> spanDecoder = new SpanDecoder("localdev");
  private ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDocoder = new SpanLogsDecoder();

  private ReportableEntityHandler<Span, String> mockTraceHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private ReportableEntityHandler<SpanLogs, String> mockTraceSpanLogsHandler =
      MockReportableEntityHandlerFactory.getMockTraceSpanLogsHandler();
  private ValidationConfiguration validationConfiguration = new ValidationConfiguration();
  private long startTime = System.currentTimeMillis();

  @Before
  public void setUp() {
    reset(mockTraceHandler, mockTraceSpanLogsHandler);
  }

  @Test
  public void testSpanLineDataBlockPreprocessor() {
    Supplier<ReportableEntityPreprocessor> preprocessorSupplier =
        () -> {
          ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
          PreprocessorRuleMetrics preprocessorRuleMetrics =
              new PreprocessorRuleMetrics(null, null, null);
          preprocessor
              .forPointLine()
              .addFilter(new LineBasedAllowFilter("^valid.*", preprocessorRuleMetrics));
          preprocessor
              .forSpan()
              .addFilter(
                  new SpanBlockFilter(SERVICE_TAG_KEY, "^test.*", null, preprocessorRuleMetrics));
          return preprocessor;
        };
    String spanLine =
        "\"valid.metric\" \"source\"=\"localdev\" "
            + "\"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" "
            + "\"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" "
            + "\"application\"=\"app\" \"service\"=\"svc\" "
            + startTime
            + " 100";

    mockTraceHandler.block(null, spanLine);
    expectLastCall();

    replay(mockTraceHandler, mockTraceSpanLogsHandler);
    preprocessAndHandleSpan(
        spanLine,
        spanDecoder,
        mockTraceHandler,
        mockTraceHandler::report,
        preprocessorSupplier,
        null,
        span -> true);
    verify(mockTraceHandler);
  }

  @Test
  public void testSpanDecodeRejectPreprocessor() {
    String spanLine =
        "\"valid.metric\" \"source\"=\"localdev\" "
            + "\"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" "
            + "\"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" "
            + "\"application\"=\"app\" \"service\"=\"svc\" "
            + startTime;

    mockTraceHandler.reject(
        spanLine, spanLine + "; reason: \"Expected timestamp, found end of " + "line\"");
    expectLastCall();

    replay(mockTraceHandler, mockTraceSpanLogsHandler);
    preprocessAndHandleSpan(
        spanLine,
        spanDecoder,
        mockTraceHandler,
        mockTraceHandler::report,
        null,
        null,
        span -> true);
    verify(mockTraceHandler);
  }

  @Test
  public void testSpanTagBlockPreprocessor() {
    Supplier<ReportableEntityPreprocessor> preprocessorSupplier =
        () -> {
          ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
          PreprocessorRuleMetrics preprocessorRuleMetrics =
              new PreprocessorRuleMetrics(null, null, null);
          preprocessor
              .forSpan()
              .addFilter(
                  new SpanBlockFilter(SERVICE_TAG_KEY, "^test.*", null, preprocessorRuleMetrics));
          return preprocessor;
        };
    String spanLine =
        "\"valid.metric\" \"source\"=\"localdev\" "
            + "\"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" "
            + "\"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" "
            + "\"application\"=\"app\" \"service\"=\"test\" "
            + startTime
            + " 100";

    mockTraceHandler.block(
        new Span(
            "valid.metric",
            "4217104a-690d-4927-baff" + "-d9aa779414c2",
            "d5355bf7-fc8d-48d1-b761-75b170f396e0",
            startTime,
            100L,
            "localdev",
            "dummy",
            ImmutableList.of(
                new Annotation("application", "app"), new Annotation("service", "test"))));

    expectLastCall();

    replay(mockTraceHandler, mockTraceSpanLogsHandler);
    preprocessAndHandleSpan(
        spanLine,
        spanDecoder,
        mockTraceHandler,
        mockTraceHandler::report,
        preprocessorSupplier,
        null,
        span -> true);
    verify(mockTraceHandler);
  }

  @Test
  public void testSpanLogsLineDataBlockPreprocessor() {
    Supplier<ReportableEntityPreprocessor> preprocessorSupplier =
        () -> {
          ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
          PreprocessorRuleMetrics preprocessorRuleMetrics =
              new PreprocessorRuleMetrics(null, null, null);
          preprocessor
              .forPointLine()
              .addFilter(new LineBasedBlockFilter(".*invalid.*", preprocessorRuleMetrics));
          return preprocessor;
        };

    String spanLine =
        "\"invalid.metric\" \"source\"=\"localdev\" "
            + "\"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" "
            + "\"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" "
            + "\"application\"=\"app\" \"service\"=\"svc\" "
            + startTime
            + " 100";
    String spanLogsLine =
        "{"
            + "\"customer\":\"dummy\","
            + "\"traceId\":\"d5355bf7-fc8d-48d1-b761-75b170f396e0\","
            + "\"spanId\":\"4217104a-690d-4927-baff-d9aa779414c2\","
            + "\"logs\":[{\"timestamp\":"
            + startTime
            + ",\"fields\":{\"error"
            + ".kind\":\"exception\", \"event\":\"error\"}}],"
            + "\"span\":\"\\\"invalid.metric\\\" \\\"source\\\"=\\\"localdev\\\" "
            + "\\\"spanId\\\"=\\\"4217104a-690d-4927-baff-d9aa779414c2\\\" "
            + "\\\"traceId\\\"=\\\"d5355bf7-fc8d-48d1-b761-75b170f396e0\\\" "
            + "\\\"application\\\"=\\\"app\\\" \\\"service\\\"=\\\"svc\\\" "
            + startTime
            + " 100\""
            + "}";
    SpanLogs spanLogs =
        SpanLogs.newBuilder()
            .setSpan(spanLine)
            .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
            .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
            .setCustomer("dummy")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setFields(
                            new HashMap<String, String>() {
                              {
                                put("error.kind", "exception");
                                put("event", "error");
                              }
                            })
                        .setTimestamp(startTime)
                        .build()))
            .build();
    mockTraceSpanLogsHandler.block(spanLogs);
    expectLastCall();

    replay(mockTraceHandler, mockTraceSpanLogsHandler);
    handleSpanLogs(
        spanLogsLine,
        spanLogsDocoder,
        spanDecoder,
        mockTraceSpanLogsHandler,
        preprocessorSupplier,
        null,
        span -> true);
    verify(mockTraceSpanLogsHandler);
  }

  @Test
  public void testSpanLogsTagBlockPreprocessor() {
    Supplier<ReportableEntityPreprocessor> preprocessorSupplier =
        () -> {
          ReportableEntityPreprocessor preprocessor = new ReportableEntityPreprocessor();
          PreprocessorRuleMetrics preprocessorRuleMetrics =
              new PreprocessorRuleMetrics(null, null, null);
          preprocessor
              .forSpan()
              .addFilter(
                  new SpanBlockFilter(SERVICE_TAG_KEY, "^test.*", null, preprocessorRuleMetrics));
          return preprocessor;
        };

    String spanLine =
        "\"invalid.metric\" \"source\"=\"localdev\" "
            + "\"spanId\"=\"4217104a-690d-4927-baff-d9aa779414c2\" "
            + "\"traceId\"=\"d5355bf7-fc8d-48d1-b761-75b170f396e0\" "
            + "\"application\"=\"app\" \"service\"=\"test\" "
            + startTime
            + " 100";
    String spanLogsLine =
        "{"
            + "\"customer\":\"dummy\","
            + "\"traceId\":\"d5355bf7-fc8d-48d1-b761-75b170f396e0\","
            + "\"spanId\":\"4217104a-690d-4927-baff-d9aa779414c2\","
            + "\"logs\":[{\"timestamp\":"
            + startTime
            + ",\"fields\":{\"error"
            + ".kind\":\"exception\", \"event\":\"error\"}}],"
            + "\"span\":\"\\\"invalid.metric\\\" \\\"source\\\"=\\\"localdev\\\" "
            + "\\\"spanId\\\"=\\\"4217104a-690d-4927-baff-d9aa779414c2\\\" "
            + "\\\"traceId\\\"=\\\"d5355bf7-fc8d-48d1-b761-75b170f396e0\\\" "
            + "\\\"application\\\"=\\\"app\\\" \\\"service\\\"=\\\"test\\\" "
            + startTime
            + " 100\""
            + "}";
    SpanLogs spanLogs =
        SpanLogs.newBuilder()
            .setSpan(spanLine)
            .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
            .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
            .setCustomer("dummy")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setFields(
                            new HashMap<String, String>() {
                              {
                                put("error.kind", "exception");
                                put("event", "error");
                              }
                            })
                        .setTimestamp(startTime)
                        .build()))
            .build();
    mockTraceSpanLogsHandler.block(spanLogs);
    expectLastCall();

    replay(mockTraceHandler, mockTraceSpanLogsHandler);
    handleSpanLogs(
        spanLogsLine,
        spanLogsDocoder,
        spanDecoder,
        mockTraceSpanLogsHandler,
        preprocessorSupplier,
        null,
        span -> true);
    verify(mockTraceSpanLogsHandler);
  }

  @Test
  public void testSpanLogsReport() {
    String spanLogsLine =
        "{"
            + "\"customer\":\"dummy\","
            + "\"traceId\":\"d5355bf7-fc8d-48d1-b761-75b170f396e0\","
            + "\"spanId\":\"4217104a-690d-4927-baff-d9aa779414c2\","
            + "\"logs\":[{\"timestamp\":"
            + startTime
            + ",\"fields\":{\"error"
            + ".kind\":\"exception\", \"event\":\"error\"}}],"
            + "\"span\":\"\\\"valid.metric\\\" \\\"source\\\"=\\\"localdev\\\" "
            + "\\\"spanId\\\"=\\\"4217104a-690d-4927-baff-d9aa779414c2\\\" "
            + "\\\"traceId\\\"=\\\"d5355bf7-fc8d-48d1-b761-75b170f396e0\\\" "
            + "\\\"application\\\"=\\\"app\\\" \\\"service\\\"=\\\"test\\\" "
            + startTime
            + " 100\""
            + "}";
    SpanLogs spanLogs =
        SpanLogs.newBuilder()
            .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
            .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
            .setCustomer("dummy")
            .setSpan("_sampledByPolicy=NONE")
            .setLogs(
                ImmutableList.of(
                    SpanLog.newBuilder()
                        .setFields(
                            new HashMap<String, String>() {
                              {
                                put("error.kind", "exception");
                                put("event", "error");
                              }
                            })
                        .setTimestamp(startTime)
                        .build()))
            .build();
    mockTraceSpanLogsHandler.report(spanLogs);
    expectLastCall();

    replay(mockTraceHandler, mockTraceSpanLogsHandler);
    handleSpanLogs(
        spanLogsLine,
        spanLogsDocoder,
        spanDecoder,
        mockTraceSpanLogsHandler,
        null,
        null,
        span -> true);
    verify(mockTraceSpanLogsHandler);
  }

  @Test
  public void testAddSpanLineWithPolicy() {
    Span span =
        Span.newBuilder()
            .setCustomer("dummy")
            .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
            .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
            .setName("spanName")
            .setStartMillis(0L)
            .setDuration(0L)
            .setAnnotations(Collections.singletonList(new Annotation("_sampledByPolicy", "test")))
            .build();
    SpanLogs spanLogs =
        SpanLogs.newBuilder()
            .setCustomer("dummy")
            .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
            .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
            .setLogs(Collections.singletonList(SpanLog.newBuilder().setTimestamp(0L).build()))
            .build();

    addSpanLine(span, spanLogs);

    assertEquals("_sampledByPolicy=test", spanLogs.getSpan());
  }

  @Test
  public void testAddSpanLineWithoutPolicy() {
    Span span =
        Span.newBuilder()
            .setCustomer("dummy")
            .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
            .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
            .setName("spanName")
            .setStartMillis(0L)
            .setDuration(0L)
            .build();
    SpanLogs spanLogs =
        SpanLogs.newBuilder()
            .setCustomer("dummy")
            .setTraceId("d5355bf7-fc8d-48d1-b761-75b170f396e0")
            .setSpanId("4217104a-690d-4927-baff-d9aa779414c2")
            .setLogs(Collections.singletonList(SpanLog.newBuilder().setTimestamp(0L).build()))
            .build();

    addSpanLine(span, spanLogs);

    assertEquals("_sampledByPolicy=NONE", spanLogs.getSpan());
  }
}
