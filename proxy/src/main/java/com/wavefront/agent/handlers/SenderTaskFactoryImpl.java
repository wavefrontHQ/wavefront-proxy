package com.wavefront.agent.handlers;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.config.ProxyRuntimeProperties;
import com.wavefront.agent.queueing.TaskSizeEstimator;
import com.wavefront.agent.queueing.TaskQueueFactory;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

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

  /**
   * Keep track of all {@link TaskSizeEstimator} instances to calculate global buffer fill rate.
   */
  private final List<TaskSizeEstimator> taskSizeEstimators = new ArrayList<>();

  private final APIContainer apiContainer;
  private final UUID proxyId;
  private final TaskQueueFactory taskQueueFactory;
  private final RecyclableRateLimiterFactory rateLimiterFactory;
  private final ProxyRuntimeProperties runtimeProperties;

  /**
   * Create new instance.
   *
   * @param apiContainer       handles interaction with Wavefront servers as well as queueing.
   * @param proxyId            proxy ID.
   * @param taskQueueFactory   factory for backing queues.
   * @param rateLimiterFactory factory for rate limiters.
   * @param runtimeProperties  container for mutable proxy settings.
   */
  public SenderTaskFactoryImpl(final APIContainer apiContainer,
                               final UUID proxyId,
                               final TaskQueueFactory taskQueueFactory,
                               @Nullable final RecyclableRateLimiterFactory rateLimiterFactory,
                               final ProxyRuntimeProperties runtimeProperties) {
    this.apiContainer = apiContainer;
    this.proxyId = proxyId;
    this.taskQueueFactory = taskQueueFactory;
    this.rateLimiterFactory = rateLimiterFactory == null ? x -> UNLIMITED : rateLimiterFactory;
    this.runtimeProperties = runtimeProperties;
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
  public Collection<SenderTask<?>> createSenderTasks(@NotNull HandlerKey handlerKey,
                                                     final int numThreads) {
    List<SenderTask<?>> toReturn = new ArrayList<>(numThreads);
    TaskSizeEstimator taskSizeEstimator = new TaskSizeEstimator(handlerKey.getHandle());
    taskSizeEstimators.add(taskSizeEstimator);
    for (int threadNo = 0; threadNo < numThreads; threadNo++) {
      SenderTask<?> senderTask;
      switch (handlerKey.getEntityType()) {
        case POINT:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.POINT,
              PUSH_FORMAT_WAVEFRONT, apiContainer.getProxyV2API(), proxyId, handlerKey.getHandle(),
              runtimeProperties, threadNo, rateLimiterFactory.getRateLimiter(handlerKey),
              taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case DELTA_COUNTER:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.DELTA_COUNTER,
              PUSH_FORMAT_WAVEFRONT, apiContainer.getProxyV2API(), proxyId, handlerKey.getHandle(),
              runtimeProperties, threadNo, rateLimiterFactory.getRateLimiter(handlerKey),
              taskSizeEstimator,
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case HISTOGRAM:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.HISTOGRAM,
              PUSH_FORMAT_HISTOGRAM, apiContainer.getProxyV2API(), proxyId, handlerKey.getHandle(),
              runtimeProperties, threadNo, rateLimiterFactory.getRateLimiter(handlerKey),
              taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case SOURCE_TAG:
          senderTask = new ReportSourceTagSenderTask(apiContainer.getSourceTagAPI(),
              handlerKey.getHandle(), threadNo, runtimeProperties,
              rateLimiterFactory.getRateLimiter(handlerKey),
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.TRACE,
              PUSH_FORMAT_TRACING, apiContainer.getProxyV2API(), proxyId, handlerKey.getHandle(),
              runtimeProperties, threadNo, rateLimiterFactory.getRateLimiter(handlerKey),
              taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE_SPAN_LOGS:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.TRACE_SPAN_LOGS,
              PUSH_FORMAT_TRACING_SPAN_LOGS, apiContainer.getProxyV2API(), proxyId,
              handlerKey.getHandle(), runtimeProperties, threadNo,
              rateLimiterFactory.getRateLimiter(handlerKey), taskSizeEstimator,
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case EVENT:
          senderTask = new EventSenderTask(apiContainer.getEventAPI(), proxyId,
              handlerKey.getHandle(), threadNo, runtimeProperties,
              rateLimiterFactory.getRateLimiter(handlerKey),
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        default:
          throw new IllegalArgumentException("Unexpected entity type " +
              handlerKey.getEntityType().name() + " for " + handlerKey.getHandle());
      }
      toReturn.add(senderTask);
      managedTasks.add(senderTask);
    }
    return toReturn;
  }

  @Override
  public void shutdown() {
    managedTasks.stream().map(SenderTask::shutdown).forEach(x -> {
      try {
        x.awaitTermination(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // ignore
      }
    });
  }

  @Override
  public void drainBuffersToQueue() {
    managedTasks.forEach(SenderTask::drainBuffersToQueue);
  }
}
