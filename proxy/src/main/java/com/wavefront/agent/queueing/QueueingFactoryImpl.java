package com.wavefront.agent.queueing;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.data.EntityWrapper;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.agent.data.TaskInjector;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.RecyclableRateLimiterFactory;
import com.wavefront.common.NamedThreadFactory;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
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

  private final Map<HandlerKey, ScheduledExecutorService> executors = new HashMap<>();
  private final Map<HandlerKey, Map<Integer, QueueProcessor<?>>> queueProcessors = new HashMap<>();
  private final Map<HandlerKey, QueueController<?>> queueControllers = new HashMap<>();
  private final TaskQueueFactory taskQueueFactory;
  private final APIContainer apiContainer;
  private final UUID proxyId;
  private final RecyclableRateLimiterFactory rateLimiterFactory;
  private final EntityWrapper entityProps;

  /**
   * @param apiContainer       handles interaction with Wavefront servers as well as queueing.
   * @param proxyId            proxy ID.
   * @param taskQueueFactory   factory for backing queues.
   * @param rateLimiterFactory factory for rate limiters.
   * @param entityProps        entity-specific wrapper over mutable proxy settings' container.
   */
  public QueueingFactoryImpl(APIContainer apiContainer,
                             UUID proxyId,
                             final TaskQueueFactory taskQueueFactory,
                             final RecyclableRateLimiterFactory rateLimiterFactory,
                             final EntityWrapper entityProps) {
    this.apiContainer = apiContainer;
    this.proxyId = proxyId;
    this.taskQueueFactory = taskQueueFactory;
    this.rateLimiterFactory = rateLimiterFactory;
    this.entityProps = entityProps;
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
            entityProps.get(handlerKey), rateLimiterFactory.getRateLimiter(handlerKey)));
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
      new QueueController<>(handlerKey, queueProcessors));
  }

  @SuppressWarnings("unchecked")
  private <T extends DataSubmissionTask<T>> TaskInjector<T> getTaskInjector(HandlerKey key,
                                                                            TaskQueue<T> queue) {
    switch (key.getEntityType()) {
      case POINT:
      case DELTA_COUNTER:
      case HISTOGRAM:
      case TRACE:
      case TRACE_SPAN_LOGS:
        return task -> ((LineDelimitedDataSubmissionTask) task).injectMembers(
            apiContainer.getProxyV2API(), proxyId, entityProps.get(key),
            (TaskQueue<LineDelimitedDataSubmissionTask>) queue);
      case SOURCE_TAG:
        return task -> ((SourceTagSubmissionTask) task).injectMembers(
            apiContainer.getSourceTagAPI(), entityProps.get(key),
            (TaskQueue<SourceTagSubmissionTask>) queue);
      case EVENT:
        return task -> ((EventDataSubmissionTask) task).injectMembers(
            apiContainer.getEventAPI(), proxyId, entityProps.get(key),
            (TaskQueue<EventDataSubmissionTask>) queue);
      default:
        throw new IllegalArgumentException("Unexpected entity type: " + key.getEntityType());
    }
  }
}
