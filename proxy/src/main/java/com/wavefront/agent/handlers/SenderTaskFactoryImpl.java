package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.common.Managed;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.queueing.QueueController;
import com.wavefront.agent.queueing.QueueingFactory;
import com.wavefront.agent.queueing.TaskSizeEstimator;
import com.wavefront.agent.queueing.TaskQueueFactory;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.wavefront.api.agent.Constants.PUSH_FORMAT_HISTOGRAM;
import static com.wavefront.api.agent.Constants.PUSH_FORMAT_TRACING;
import static com.wavefront.api.agent.Constants.PUSH_FORMAT_TRACING_SPAN_LOGS;
import static com.wavefront.api.agent.Constants.PUSH_FORMAT_WAVEFRONT;

/**
 * Factory for {@link SenderTask} objects.
 *
 * @author vasily@wavefront.com
 */
public class SenderTaskFactoryImpl implements SenderTaskFactory {

  private final Map<String, List<ReportableEntityType>> entityTypes = new ConcurrentHashMap<>();
  private final Map<HandlerKey, ScheduledExecutorService> executors = new ConcurrentHashMap<>();
  private final Map<HandlerKey, List<SenderTask<?>>> managedTasks = new ConcurrentHashMap<>();
  private final Map<HandlerKey, Managed> managedServices = new ConcurrentHashMap<>();

  /**
   * Keep track of all {@link TaskSizeEstimator} instances to calculate global buffer fill rate.
   */
  private final Map<HandlerKey, TaskSizeEstimator> taskSizeEstimators = new ConcurrentHashMap<>();

  private final APIContainer apiContainer;
  private final UUID proxyId;
  private final TaskQueueFactory taskQueueFactory;
  private final QueueingFactory queueingFactory;
  private final EntityPropertiesFactory entityPropsFactory;

  /**
   * Create new instance.
   *
   * @param apiContainer       handles interaction with Wavefront servers as well as queueing.
   * @param proxyId            proxy ID.
   * @param taskQueueFactory   factory for backing queues.
   * @param queueingFactory    factory for queueing.
   * @param entityPropsFactory factory for entity-specific wrappers for mutable proxy settings.
   */
  public SenderTaskFactoryImpl(final APIContainer apiContainer,
                               final UUID proxyId,
                               final TaskQueueFactory taskQueueFactory,
                               @Nullable final QueueingFactory queueingFactory,
                               final EntityPropertiesFactory entityPropsFactory) {
    this.apiContainer = apiContainer;
    this.proxyId = proxyId;
    this.taskQueueFactory = taskQueueFactory;
    this.queueingFactory = queueingFactory;
    this.entityPropsFactory = entityPropsFactory;
    // global `~proxy.buffer.fill-rate` metric aggregated from all task size estimators
    Metrics.newGauge(new TaggedMetricName("buffer", "fill-rate"),
        new Gauge<Long>() {
          @Override
          public Long value() {
            List<Long> sizes = taskSizeEstimators.values().stream().
                map(TaskSizeEstimator::getBytesPerMinute).filter(Objects::nonNull).
                collect(Collectors.toList());
            return sizes.size() == 0 ? null : sizes.stream().mapToLong(x -> x).sum();
          }
        });
  }

  @SuppressWarnings("unchecked")
  public Collection<SenderTask<?>> createSenderTasks(@Nonnull HandlerKey handlerKey) {
    ReportableEntityType entityType = handlerKey.getEntityType();
    int numThreads = entityPropsFactory.get(entityType).getFlushThreads();
    List<SenderTask<?>> toReturn = new ArrayList<>(numThreads);
    TaskSizeEstimator taskSizeEstimator = new TaskSizeEstimator(handlerKey.getHandle());
    taskSizeEstimators.put(handlerKey, taskSizeEstimator);

    ScheduledExecutorService scheduler = executors.computeIfAbsent(handlerKey, x ->
        Executors.newScheduledThreadPool(numThreads, new NamedThreadFactory("submitter-" +
            handlerKey.getEntityType() + "-" + handlerKey.getHandle())));

    for (int threadNo = 0; threadNo < numThreads; threadNo++) {
      SenderTask<?> senderTask;
      switch (entityType) {
        case POINT:
        case DELTA_COUNTER:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_WAVEFRONT,
              apiContainer.getProxyV2API(), proxyId, entityPropsFactory.get(entityType), scheduler,
              threadNo, taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case HISTOGRAM:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_HISTOGRAM,
              apiContainer.getProxyV2API(), proxyId, entityPropsFactory.get(entityType), scheduler,
              threadNo, taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case SOURCE_TAG:
          senderTask = new SourceTagSenderTask(handlerKey, apiContainer.getSourceTagAPI(),
              threadNo, entityPropsFactory.get(entityType), scheduler,
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_TRACING,
              apiContainer.getProxyV2API(), proxyId, entityPropsFactory.get(entityType), scheduler,
              threadNo, taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE_SPAN_LOGS:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_TRACING_SPAN_LOGS,
              apiContainer.getProxyV2API(), proxyId, entityPropsFactory.get(entityType), scheduler,
              threadNo, taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case EVENT:
          senderTask = new EventSenderTask(handlerKey, apiContainer.getEventAPI(), proxyId,
              threadNo, entityPropsFactory.get(entityType), scheduler,
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        default:
          throw new IllegalArgumentException("Unexpected entity type " +
              handlerKey.getEntityType().name() + " for " + handlerKey.getHandle());
      }
      toReturn.add(senderTask);
      senderTask.start();
    }
    if (queueingFactory != null) {
      QueueController<?> controller = queueingFactory.getQueueController(handlerKey, numThreads);
      managedServices.put(handlerKey, controller);
      controller.start();
    }
    managedTasks.put(handlerKey, toReturn);
    entityTypes.computeIfAbsent(handlerKey.getHandle(), x -> new ArrayList<>()).
        add(handlerKey.getEntityType());
    return toReturn;
  }

  @Override
  public void shutdown() {
    managedTasks.values().stream().flatMap(Collection::stream).forEach(Managed::stop);
    taskSizeEstimators.values().forEach(TaskSizeEstimator::shutdown);
    managedServices.values().forEach(Managed::stop);
    executors.values().forEach(x -> {
      try {
        x.shutdown();
        x.awaitTermination(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // ignore
      }
    });
  }

  @Override
  public void shutdown(@Nonnull String handle) {
    List<ReportableEntityType> types = entityTypes.get(handle);
    if (types == null) return;
    try {
      types.forEach(x -> taskSizeEstimators.remove(HandlerKey.of(x, handle)).shutdown());
      types.forEach(x -> managedServices.remove(HandlerKey.of(x, handle)).stop());
      types.forEach(x -> managedTasks.remove(HandlerKey.of(x, handle)).forEach(t -> {
        t.stop();
        t.drainBuffersToQueue(null);
      }));
      types.forEach(x -> executors.remove(HandlerKey.of(x, handle)).shutdown());
    } finally {
      entityTypes.remove(handle);
    }
  }

  @Override
  public void drainBuffersToQueue(QueueingReason reason) {
    managedTasks.values().stream().flatMap(Collection::stream).
        forEach(x -> x.drainBuffersToQueue(reason));
  }

  @VisibleForTesting
  public void flushNow(@Nonnull HandlerKey handlerKey) {
    managedTasks.get(handlerKey).forEach(task -> {
      if (task instanceof AbstractSenderTask) {
        ((AbstractSenderTask<?>) task).run();
      }
    });
  }
}
