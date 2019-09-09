package com.wavefront.agent.handlers;

import org.easymock.EasyMock;

import wavefront.report.ReportPoint;
import wavefront.report.ReportSourceTag;
import wavefront.report.Span;
import wavefront.report.SpanLogs;


/**
 * Mock factory for testing
 *
 * @author vasily@wavefront.com
 */
public class MockReportableEntityHandlerFactory {

  public static ReportPointHandlerImpl getMockReportPointHandler() {
    return EasyMock.createMock(ReportPointHandlerImpl.class);
  }

  public static ReportSourceTagHandlerImpl getMockSourceTagHandler() {
    return EasyMock.createMock(ReportSourceTagHandlerImpl.class);
  }

  public static ReportPointHandlerImpl getMockHistogramHandler() {
    return EasyMock.createMock(ReportPointHandlerImpl.class);
  }

  public static SpanHandlerImpl getMockTraceHandler() {
    return EasyMock.createMock(SpanHandlerImpl.class);
  }

  public static SpanLogsHandlerImpl getMockTraceSpanLogsHandler() {
    return EasyMock.createMock(SpanLogsHandlerImpl.class);
  }

  public static ReportableEntityHandlerFactory createMockHandlerFactory(
      ReportableEntityHandler<ReportPoint> mockReportPointHandler,
      ReportableEntityHandler<ReportSourceTag> mockSourceTagHandler,
      ReportableEntityHandler<ReportPoint> mockHistogramHandler,
      ReportableEntityHandler<Span> mockTraceHandler,
      ReportableEntityHandler<SpanLogs> mockTraceSpanLogsHandler) {
    return handlerKey -> {
      switch (handlerKey.getEntityType()) {
        case POINT:
          return mockReportPointHandler;
        case SOURCE_TAG:
          return mockSourceTagHandler;
        case HISTOGRAM:
          return mockHistogramHandler;
        case TRACE:
          return mockTraceHandler;
        case TRACE_SPAN_LOGS:
          return mockTraceSpanLogsHandler;
        default:
          throw new IllegalArgumentException("Unknown entity type");
      }
    };
  }

}
