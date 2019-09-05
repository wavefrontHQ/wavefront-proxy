package com.wavefront.agent.handlers;

import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.data.ReportableEntityType;
import org.easymock.EasyMock;

import wavefront.report.ReportPoint;
import wavefront.report.ReportSourceTag;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

import java.util.Collection;
import java.util.function.Supplier;

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

  public static DeltaCounterHandlerImpl getMockDeltaCounterHandler(String handle, Collection<SenderTask> senderTasks,
         Supplier<ValidationConfiguration> validationConfig, long reportIntervalSeconds) throws InterruptedException {
    return new DeltaCounterHandlerImpl(handle, 5, senderTasks, validationConfig,
            ReportableEntityType.DELTA_COUNTER, true, reportIntervalSeconds);
  }

  public static ReportableEntityHandlerFactory createMockHandlerFactory(
      ReportableEntityHandler<ReportPoint> mockReportPointHandler,
      ReportableEntityHandler<ReportPoint> mockDeltaCounterHandler,
      ReportableEntityHandler<ReportSourceTag> mockSourceTagHandler,
      ReportableEntityHandler<ReportPoint> mockHistogramHandler,
      ReportableEntityHandler<Span> mockTraceHandler,
      ReportableEntityHandler<SpanLogs> mockTraceSpanLogsHandler) {
    return handlerKey -> {
      switch (handlerKey.getEntityType()) {
        case POINT:
          return mockReportPointHandler;
        case DELTA_COUNTER:
          return mockDeltaCounterHandler;
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
