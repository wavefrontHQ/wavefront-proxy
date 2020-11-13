package com.wavefront.agent.queueing;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.agent.data.TaskInjector;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.data.ReportableEntityType;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A caching implementation of {@link QueueingFactory}.
 *
 * @author vasily@wavefront.com
 */
public class QueueingFactoryImpl implements QueueingFactory {

  private final Map<HandlerKey, ScheduledExecutorService> executors = new ConcurrentHashMap<>();
  private final Map<HandlerKey, Map<Integer, QueueProcessor<?>>> queueProcessors =
      new ConcurrentHashMap<>();
  private final Map<HandlerKey, QueueController<?>> queueControllers = new ConcurrentHashMap<>();
  private final TaskQueueFactory taskQueueFactory;
  private final APIContainer apiContainer;
  private final UUID proxyId;
  private final EntityPropertiesFactory entityPropsFactory;

  /**
   * @param apiContainer       handles interaction with Wavefront servers as well as queueing.
   * @param proxyId            proxy ID.
   * @param taskQueueFactory   factory for backing queues.
   * @param entityPropsFactory factory for entity-specific wrappers for mutable proxy settings.
   */
  public QueueingFactoryImpl(APIContainer apiContainer,
                             UUID proxyId,
                             final TaskQueueFactory taskQueueFactory,
                             final EntityPropertiesFactory entityPropsFactory) {
    this.apiContainer = apiContainer;
    this.proxyId = proxyId;
    this.taskQueueFactory = taskQueueFactory;
    this.entityPropsFactory = entityPropsFactory;
  }

  /**
   * Create a new {@code QueueProcessor} instance for the specified handler key.
   *
   * @param handlerKey      {@link HandlerKey} for the queue processor.
   * @param executorService executor service
   * @param threadNum       thread number
   * @param <T>             data submission task type
   * @return {@code QueueProcessor} object
   */
  <T extends DataSubmissionTask<T>> QueueProcessor<T> getQueueProcessor(
      @Nonnull HandlerKey handlerKey, ScheduledExecutorService executorService, int threadNum) {
    TaskQueue<T> taskQueue = taskQueueFactory.getTaskQueue(handlerKey, threadNum);
    //noinspection unchecked
    return (QueueProcessor<T>) queueProcessors.computeIfAbsent(handlerKey, x -> new TreeMap<>()).
        computeIfAbsent(threadNum, x -> new QueueProcessor<>(handlerKey, taskQueue,
            getTaskInjector(handlerKey, taskQueue), executorService,
            entityPropsFactory.get(handlerKey.getEntityType()),
            entityPropsFactory.getGlobalProperties()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends DataSubmissionTask<T>> QueueController<T> getQueueController(
      @Nonnull HandlerKey handlerKey, int numThreads) {
    ScheduledExecutorService executor = executors.computeIfAbsent(handlerKey, x ->
        Executors.newScheduledThreadPool(numThreads, new NamedThreadFactory("queueProcessor-" +
            handlerKey.getEntityType() + "-" + handlerKey.getHandle())));
    List<QueueProcessor<T>> queueProcessors = IntStream.range(0, numThreads).
        mapToObj(i -> (QueueProcessor<T>) getQueueProcessor(handlerKey, executor, i)).
        collect(Collectors.toList());
    return (QueueController<T>) queueControllers.computeIfAbsent(handlerKey, x ->
      new QueueController<>(handlerKey, queueProcessors,
          backlogSize -> entityPropsFactory.get(handlerKey.getEntityType()).
              reportBacklogSize(handlerKey.getHandle(), backlogSize)));
  }

  @SuppressWarnings("unchecked")
  private <T extends DataSubmissionTask<T>> TaskInjector<T> getTaskInjector(HandlerKey handlerKey,
                                                                            TaskQueue<T> queue) {
    ReportableEntityType entityType = handlerKey.getEntityType();
    switch (entityType) {
      case POINT:
      case DELTA_COUNTER:
      case HISTOGRAM:
      case TRACE:
      case TRACE_SPAN_LOGS:
        return task -> ((LineDelimitedDataSubmissionTask) task).injectMembers(
            apiContainer.getProxyV2API(), proxyId, entityPropsFactory.get(entityType),
            (TaskQueue<LineDelimitedDataSubmissionTask>) queue);
      case SOURCE_TAG:
        return task -> ((SourceTagSubmissionTask) task).injectMembers(
            apiContainer.getSourceTagAPI(), entityPropsFactory.get(entityType),
            (TaskQueue<SourceTagSubmissionTask>) queue);
      case EVENT:
        return task -> ((EventDataSubmissionTask) task).injectMembers(
            apiContainer.getEventAPI(), proxyId, entityPropsFactory.get(entityType),
            (TaskQueue<EventDataSubmissionTask>) queue);
      default:
        throw new IllegalArgumentException("Unexpected entity type: " + entityType);
    }
  }

  @VisibleForTesting
  public void flushNow(@Nonnull HandlerKey handlerKey) {
    queueProcessors.get(handlerKey).values().forEach(QueueProcessor::run);
  }
}
