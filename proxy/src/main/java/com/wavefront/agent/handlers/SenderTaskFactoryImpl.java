package com.wavefront.agent.handlers;

import com.wavefront.common.Managed;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.data.EntityWrapper;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.queueing.QueueController;
import com.wavefront.agent.queueing.QueueingFactory;
import com.wavefront.agent.queueing.TaskSizeEstimator;
import com.wavefront.agent.queueing.TaskQueueFactory;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.wavefront.agent.handlers.RecyclableRateLimiterFactoryImpl.UNLIMITED;
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

  private final List<SenderTask<?>> managedTasks = new ArrayList<>();
  private final List<Managed> managedServices = new ArrayList<>();

  /**
   * Keep track of all {@link TaskSizeEstimator} instances to calculate global buffer fill rate.
   */
  private final List<TaskSizeEstimator> taskSizeEstimators = new ArrayList<>();

  private final APIContainer apiContainer;
  private final UUID proxyId;
  private final TaskQueueFactory taskQueueFactory;
  private final QueueingFactory queueingFactory;
  private final RecyclableRateLimiterFactory rateLimiterFactory;
  private final EntityWrapper entityProps;

  /**
   * Create new instance.
   *
   * @param apiContainer       handles interaction with Wavefront servers as well as queueing.
   * @param proxyId            proxy ID.
   * @param taskQueueFactory   factory for backing queues.
   * @param queueingFactory    factory for queueing.
   * @param rateLimiterFactory factory for rate limiters.
   * @param entityProps        entity-specific wrapper over mutable proxy settings' container.
   */
  public SenderTaskFactoryImpl(final APIContainer apiContainer,
                               final UUID proxyId,
                               final TaskQueueFactory taskQueueFactory,
                               @Nullable final QueueingFactory queueingFactory,
                               @Nullable final RecyclableRateLimiterFactory rateLimiterFactory,
                               final EntityWrapper entityProps) {
    this.apiContainer = apiContainer;
    this.proxyId = proxyId;
    this.taskQueueFactory = taskQueueFactory;
    this.queueingFactory = queueingFactory;
    this.rateLimiterFactory = rateLimiterFactory == null ? x -> UNLIMITED : rateLimiterFactory;
    this.entityProps = entityProps;
    // global `~proxy.buffer.fill-rate` metric aggregated from all task size estimators
    Metrics.newGauge(new TaggedMetricName("buffer", "fill-rate"),
        new Gauge<Long>() {
          @Override
          public Long value() {
            List<Long> sizes = taskSizeEstimators.stream().
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
    taskSizeEstimators.add(taskSizeEstimator);
    for (int threadNo = 0; threadNo < numThreads; threadNo++) {
      SenderTask<?> senderTask;
      switch (handlerKey.getEntityType()) {
        case POINT:
        case DELTA_COUNTER:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_WAVEFRONT,
              apiContainer.getProxyV2API(), proxyId, entityProps.get(handlerKey), threadNo,
              rateLimiterFactory.getRateLimiter(handlerKey), taskSizeEstimator,
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case HISTOGRAM:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_HISTOGRAM,
              apiContainer.getProxyV2API(), proxyId, entityProps.get(handlerKey), threadNo,
              rateLimiterFactory.getRateLimiter(handlerKey), taskSizeEstimator,
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case SOURCE_TAG:
          senderTask = new ReportSourceTagSenderTask(handlerKey, apiContainer.getSourceTagAPI(),
              threadNo, entityProps.get(handlerKey), rateLimiterFactory.getRateLimiter(handlerKey),
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_TRACING,
              apiContainer.getProxyV2API(), proxyId, entityProps.get(handlerKey), threadNo,
              rateLimiterFactory.getRateLimiter(handlerKey), taskSizeEstimator,
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE_SPAN_LOGS:
          senderTask = new LineDelimitedSenderTask(handlerKey, PUSH_FORMAT_TRACING_SPAN_LOGS,
              apiContainer.getProxyV2API(), proxyId, entityProps.get(handlerKey), threadNo,
              rateLimiterFactory.getRateLimiter(handlerKey), taskSizeEstimator,
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case EVENT:
          senderTask = new EventSenderTask(handlerKey, apiContainer.getEventAPI(), proxyId,
              threadNo, entityProps.get(handlerKey), rateLimiterFactory.getRateLimiter(handlerKey),
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        default:
          throw new IllegalArgumentException("Unexpected entity type " +
              handlerKey.getEntityType().name() + " for " + handlerKey.getHandle());
      }
      toReturn.add(senderTask);
      managedTasks.add(senderTask);
    }
    if (queueingFactory != null) {
      QueueController<?> controller = queueingFactory.getQueueController(handlerKey, numThreads);
      managedServices.add(controller);
      controller.start();
    }
    return toReturn;
  }

  @Override
  public void shutdown() {
    managedServices.forEach(Managed::stop);
    managedTasks.stream().map(SenderTask::shutdown).forEach(x -> {
      try {
        x.awaitTermination(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // ignore
      }
    });
  }

  @Override
  public void drainBuffersToQueue(QueueingReason reason) {
    managedTasks.forEach(x -> x.drainBuffersToQueue(reason));
  }
}
