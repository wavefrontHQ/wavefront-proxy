package com.wavefront.agent.handlers;

import com.wavefront.api.agent.ValidationConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * Caching factory for {@link ReportableEntityHandler} objects. Makes sure there's only one handler
 * for each {@link HandlerKey}, which makes it possible to spin up handlers on demand at runtime,
 * as well as redirecting traffic to a different pipeline.
 *
 * @author vasily@wavefront.com
 */
public class ReportableEntityHandlerFactoryImpl implements ReportableEntityHandlerFactory {
  private static final Logger logger = Logger.getLogger(
      ReportableEntityHandlerFactoryImpl.class.getCanonicalName());

  private static final int SOURCE_TAG_API_NUM_THREADS = 2;
  private static final int EVENT_API_NUM_THREADS = 2;

  protected final Map<HandlerKey, ReportableEntityHandler<?, ?>> handlers = new HashMap<>();

  private final SenderTaskFactory senderTaskFactory;
  private final int blockedItemsPerBatch;
  private final int defaultFlushThreads;
  private final Supplier<ValidationConfiguration> validationConfig;
  private final Logger blockedPointsLogger;
  private final Logger blockedHistogramsLogger;
  private final Logger blockedSpansLogger;

  /**
   * Create new instance.
   *
   * @param senderTaskFactory    SenderTaskFactory instance used to create SenderTasks
   *                             for new handlers.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written
   *                             into the main log file.
   * @param defaultFlushThreads  control fanout for SenderTasks.
   * @param validationConfig     Supplier for the ValidationConfiguration.
   */
  public ReportableEntityHandlerFactoryImpl(
      final SenderTaskFactory senderTaskFactory, final int blockedItemsPerBatch,
      final int defaultFlushThreads,
      @Nullable final Supplier<ValidationConfiguration> validationConfig,
      final Logger blockedPointsLogger, final Logger blockedHistogramsLogger,
      final Logger blockedSpansLogger) {
    this.senderTaskFactory = senderTaskFactory;
    this.blockedItemsPerBatch = blockedItemsPerBatch;
    this.defaultFlushThreads = defaultFlushThreads;
    this.validationConfig = validationConfig;
    this.blockedPointsLogger = blockedPointsLogger;
    this.blockedHistogramsLogger = blockedHistogramsLogger;
    this.blockedSpansLogger = blockedSpansLogger;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T, U> ReportableEntityHandler<T, U> getHandler(HandlerKey handlerKey) {
    return (ReportableEntityHandler<T, U>) handlers.computeIfAbsent(handlerKey, k -> {
      switch (handlerKey.getEntityType()) {
        case POINT:
          return new ReportPointHandlerImpl(handlerKey.getHandle(), blockedItemsPerBatch,
              senderTaskFactory.createSenderTasks(handlerKey, defaultFlushThreads),
              validationConfig, false, true, blockedPointsLogger);
        case HISTOGRAM:
          return new ReportPointHandlerImpl(handlerKey.getHandle(), blockedItemsPerBatch,
              senderTaskFactory.createSenderTasks(handlerKey, defaultFlushThreads),
              validationConfig, true, true, blockedHistogramsLogger);
        case SOURCE_TAG:
          return new ReportSourceTagHandlerImpl(handlerKey.getHandle(), blockedItemsPerBatch,
              senderTaskFactory.createSenderTasks(handlerKey, SOURCE_TAG_API_NUM_THREADS),
              blockedPointsLogger);
        case TRACE:
          return new SpanHandlerImpl(handlerKey.getHandle(), blockedItemsPerBatch,
              senderTaskFactory.createSenderTasks(handlerKey, defaultFlushThreads),
              validationConfig, blockedSpansLogger);
        case TRACE_SPAN_LOGS:
          return new SpanLogsHandlerImpl(handlerKey.getHandle(), blockedItemsPerBatch,
              senderTaskFactory.createSenderTasks(handlerKey, defaultFlushThreads),
              blockedSpansLogger);
        case EVENT:
          return new EventHandlerImpl(handlerKey.getHandle(), blockedItemsPerBatch,
              senderTaskFactory.createSenderTasks(handlerKey, EVENT_API_NUM_THREADS),
              blockedPointsLogger);
        default:
          throw new IllegalArgumentException("Unexpected entity type " +
              handlerKey.getEntityType().name() + " for " + handlerKey.getHandle());
      }
    });
  }

  @Override
  public void shutdown() {
    //
  }
}
