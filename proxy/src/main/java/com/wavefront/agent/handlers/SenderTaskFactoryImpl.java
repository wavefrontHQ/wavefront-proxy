package com.wavefront.agent.handlers;

import com.wavefront.common.Managed;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.queueing.QueueController;
import com.wavefront.agent.queueing.QueueingFactory;
import com.wavefront.agent.queueing.TaskSizeEstimator;
import com.wavefront.agent.queueing.TaskQueueFactory;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
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

  private final Map<HandlerKey, List<SenderTask<?>>> managedTasks = new HashMap<>();
  private final Map<HandlerKey, Managed> managedServices = new HashMap<>();

  /**
   * Keep track of all {@link TaskSizeEstimator} instances to calculate global buffer fill rate.
   */
  private final Map<HandlerKey, TaskSizeEstimator> taskSizeEstimators = new HashMap<>();

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
  public Collection<SenderTask<?>> createSenderTasks(@Nonnull HandlerKey handlerKey,
                                                     final int numThreads) {
    List<SenderTask<?>> toReturn = new ArrayList<>(numThreads);
    TaskSizeEstimator taskSizeEstimator = new TaskSizeEstimator(handlerKey.getHandle());
    taskSizeEstimators.put(handlerKey, taskSizeEstimator);
    ReportableEntityType entityType = handlerKey.getEntityType();
    for (int threadNo = 0; threadNo < numThreads; threadNo++) {
      SenderTask<?> senderTask;
      switch (entityType) {
        case POINT:
        case DELTA_COUNTER:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_WAVEFRONT,
              apiContainer.getProxyV2API(), proxyId, entityPropsFactory.get(entityType), threadNo,
              taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case HISTOGRAM:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_HISTOGRAM,
              apiContainer.getProxyV2API(), proxyId, entityPropsFactory.get(entityType), threadNo,
              taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case SOURCE_TAG:
          senderTask = new ReportSourceTagSenderTask(handlerKey, apiContainer.getSourceTagAPI(),
              threadNo, entityPropsFactory.get(entityType),
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_TRACING,
              apiContainer.getProxyV2API(), proxyId, entityPropsFactory.get(entityType), threadNo,
              taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE_SPAN_LOGS:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_TRACING_SPAN_LOGS,
              apiContainer.getProxyV2API(), proxyId, entityPropsFactory.get(entityType), threadNo,
              taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case EVENT:
          senderTask = new EventSenderTask(handlerKey, apiContainer.getEventAPI(), proxyId,
              threadNo, entityPropsFactory.get(entityType),
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        default:
          throw new IllegalArgumentException("Unexpected entity type " +
              handlerKey.getEntityType().name() + " for " + handlerKey.getHandle());
      }
      toReturn.add(senderTask);
    }
    if (queueingFactory != null) {
      QueueController<?> controller = queueingFactory.getQueueController(handlerKey, numThreads);
      managedServices.put(handlerKey, controller);
      controller.start();
    }
    managedTasks.put(handlerKey, toReturn);
    return toReturn;
  }

  @Override
  public void shutdown() {
    taskSizeEstimators.values().forEach(TaskSizeEstimator::shutdown);
    managedServices.values().forEach(Managed::stop);
    managedTasks.values().stream().flatMap(Collection::stream).map(SenderTask::shutdown).
        forEach(x -> {
          try {
            x.awaitTermination(1000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            // ignore
          }
        });
  }

  @Override
  public void shutdown(@Nonnull String handle) {
    taskSizeEstimators.entrySet().stream().filter(x -> x.getKey().getHandle().equals(handle)).
        collect(Collectors.toList()).forEach(x -> taskSizeEstimators.remove(x.getKey()).shutdown());
    managedServices.entrySet().stream().filter(x -> x.getKey().getHandle().equals(handle)).
        collect(Collectors.toList()).forEach(x -> managedServices.remove(x.getKey()).stop());
    managedTasks.entrySet().stream().filter(x -> x.getKey().getHandle().equals(handle)).
        collect(Collectors.toList()).forEach(x -> managedTasks.remove(x.getKey()).forEach(e -> {
          try {
            e.drainBuffersToQueue(null);
            e.shutdown().awaitTermination(1000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException ex) {
            // ignore
          }
    }));
  }

  @Override
  public void drainBuffersToQueue(QueueingReason reason) {
    managedTasks.values().stream().flatMap(Collection::stream).
        forEach(x -> x.drainBuffersToQueue(reason));
  }
}
