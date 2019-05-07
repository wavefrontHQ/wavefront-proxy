package com.wavefront.agent.handlers;

import java.util.HashMap;
import java.util.Map;

public class SampleCustomerReportableEntityHandlerFactoryImpl extends ReportableEntityHandlerFactoryImpl {

  private Map<HandlerKey, ReportableEntityHandler> handlers = new HashMap<>();

  public SampleCustomerReportableEntityHandlerFactoryImpl(final SenderTaskFactory senderTaskFactory,
                                                          final int blockedItemsPerBatch,
                                                          final int defaultFlushThreads) {
    super(senderTaskFactory, blockedItemsPerBatch, defaultFlushThreads);
  }

  @Override
  public ReportableEntityHandler getHandler(HandlerKey handlerKey) {
    return handlers.computeIfAbsent(handlerKey, k -> {
      switch (handlerKey.getEntityType()) {
        case POINT:
        case HISTOGRAM:
          return new SampleCustomerReportPointHandlerImpl(handlerKey.getHandle(),
              blockedItemsPerBatch, senderTaskFactory.createSenderTasks(handlerKey,
              defaultFlushThreads));
        case SOURCE_TAG:
          return new ReportSourceTagHandlerImpl(handlerKey.getHandle(), blockedItemsPerBatch,
              senderTaskFactory.createSenderTasks(handlerKey, SOURCE_TAGS_NUM_THREADS));
        case TRACE:
          return new SampleCustomerSpanHandlerImpl(handlerKey.getHandle(), blockedItemsPerBatch,
              senderTaskFactory.createSenderTasks(handlerKey, defaultFlushThreads));
        case TRACE_SPAN_LOGS:
          return new SpanLogsHandlerImpl(handlerKey.getHandle(), blockedItemsPerBatch,
              senderTaskFactory.createSenderTasks(handlerKey, defaultFlushThreads));
        default:
          throw new IllegalArgumentException("Unexpected entity type " + handlerKey.getEntityType().name() +
              " for " + handlerKey.getHandle());
      }
    });

  }
}