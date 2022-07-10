package com.wavefront.agent.core.handlers;

import com.wavefront.agent.core.queues.QueueInfo;
import javax.annotation.Nonnull;
import org.easymock.EasyMock;
import wavefront.report.*;

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
      ReportableEntityHandler<ReportPoint> mockReportPointHandler,
      ReportableEntityHandler<ReportSourceTag> mockSourceTagHandler,
      ReportableEntityHandler<ReportPoint> mockHistogramHandler,
      ReportableEntityHandler<Span> mockTraceHandler,
      ReportableEntityHandler<SpanLogs> mockTraceSpanLogsHandler,
      ReportableEntityHandler<ReportEvent> mockEventHandler) {
    return new ReportableEntityHandlerFactory() {
      @SuppressWarnings("unchecked")
      @Override
      public <T> ReportableEntityHandler<T> getHandler(String handle, QueueInfo handlerKey) {
        switch (handlerKey.getEntityType()) {
          case POINT:
            return (ReportableEntityHandler<T>) mockReportPointHandler;
          case SOURCE_TAG:
            return (ReportableEntityHandler<T>) mockSourceTagHandler;
          case HISTOGRAM:
            return (ReportableEntityHandler<T>) mockHistogramHandler;
          case TRACE:
            return (ReportableEntityHandler<T>) mockTraceHandler;
          case TRACE_SPAN_LOGS:
            return (ReportableEntityHandler<T>) mockTraceSpanLogsHandler;
          case EVENT:
            return (ReportableEntityHandler<T>) mockEventHandler;
          default:
            throw new IllegalArgumentException("Unknown entity type");
        }
      }

      @Override
      public void shutdown(@Nonnull int handle) {}
    };
  }
}
