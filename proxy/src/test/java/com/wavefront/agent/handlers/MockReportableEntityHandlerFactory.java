package com.wavefront.agent.handlers;

import com.wavefront.dto.Event;
import com.wavefront.dto.SourceTag;
import org.easymock.EasyMock;

import wavefront.report.ReportEvent;
import wavefront.report.ReportPoint;
import wavefront.report.ReportSourceTag;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

import javax.annotation.Nonnull;

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

  public static EventHandlerImpl getMockEventHandlerImpl() {
    return EasyMock.createMock(EventHandlerImpl.class);
  }


  public static ReportableEntityHandlerFactory createMockHandlerFactory(
      ReportableEntityHandler<ReportPoint, String> mockReportPointHandler,
      ReportableEntityHandler<ReportSourceTag, SourceTag> mockSourceTagHandler,
      ReportableEntityHandler<ReportPoint, String> mockHistogramHandler,
      ReportableEntityHandler<Span, String> mockTraceHandler,
      ReportableEntityHandler<SpanLogs, String> mockTraceSpanLogsHandler,
      ReportableEntityHandler<ReportEvent, Event> mockEventHandler) {
    return new ReportableEntityHandlerFactory() {
      @SuppressWarnings("unchecked")
      @Override
      public <T, U> ReportableEntityHandler<T, U> getHandler(HandlerKey handlerKey) {
        switch (handlerKey.getEntityType()) {
          case POINT:
            return (ReportableEntityHandler<T, U>) mockReportPointHandler;
          case SOURCE_TAG:
            return (ReportableEntityHandler<T, U>) mockSourceTagHandler;
          case HISTOGRAM:
            return (ReportableEntityHandler<T, U>) mockHistogramHandler;
          case TRACE:
            return (ReportableEntityHandler<T, U>) mockTraceHandler;
          case TRACE_SPAN_LOGS:
            return (ReportableEntityHandler<T, U>) mockTraceSpanLogsHandler;
          case EVENT:
            return (ReportableEntityHandler<T, U>) mockEventHandler;
          default:
            throw new IllegalArgumentException("Unknown entity type");
        }
      }

      @Override
      public void shutdown(@Nonnull String handle) {
      }
    };
  }
}
