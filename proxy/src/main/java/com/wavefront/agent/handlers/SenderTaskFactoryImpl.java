package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiterImpl;
import com.wavefront.agent.api.APIContainer;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

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

  private List<SenderTask> managedTasks = new ArrayList<>();

  /**
   * Keep track of all {@link TaskSizeEstimator} instances to calculate global buffer fill rate.
   */
  private List<TaskSizeEstimator> taskSizeEstimators = new ArrayList<>();

  private final APIContainer apiContainer;
  private final UUID proxyId;
  private final TaskQueueFactory taskQueueFactory;
  private final RecyclableRateLimiter globalRateLimiter;
  private final AtomicInteger pushFlushInterval;
  private final AtomicInteger pointsPerBatch;
  private final AtomicInteger memoryBufferLimit;

  // TODO: Implement using source tag rate limit from the backend
  public static final RecyclableRateLimiter SOURCE_TAG_RATE_LIMITER =
      RecyclableRateLimiterImpl.create(5, 10);
  // TODO: Implement using event rate limit from the backend
  public static final RecyclableRateLimiter EVENT_RATE_LIMITER =
      RecyclableRateLimiterImpl.create(5, 10);

  /**
   * Create new instance.
   *
   * @param apiContainer      handles interaction with Wavefront servers as well as queueing.
   * @param proxyId           proxy ID.
   * @param taskQueueFactory
   * @param globalRateLimiter rate limiter to control outbound point rate.
   * @param pushFlushInterval interval between flushes.
   * @param itemsPerBatch     max points per flush.
   * @param memoryBufferLimit max points in task's memory buffer before queueing.
   */
  public SenderTaskFactoryImpl(final APIContainer apiContainer,
                               final UUID proxyId,
                               final TaskQueueFactory taskQueueFactory,
                               final RecyclableRateLimiter globalRateLimiter,
                               final AtomicInteger pushFlushInterval,
                               @Nullable final AtomicInteger itemsPerBatch,
                               @Nullable final AtomicInteger memoryBufferLimit) {
    this.apiContainer = apiContainer;
    this.proxyId = proxyId;
    this.taskQueueFactory = taskQueueFactory;
    this.globalRateLimiter = globalRateLimiter;
    this.pushFlushInterval = pushFlushInterval;
    this.pointsPerBatch = itemsPerBatch;
    this.memoryBufferLimit = memoryBufferLimit;
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
  public Collection<SenderTask> createSenderTasks(@NotNull HandlerKey handlerKey,
                                                  final int numThreads) {
    List<SenderTask> toReturn = new ArrayList<>(numThreads);
    TaskSizeEstimator taskSizeEstimator = new TaskSizeEstimator(handlerKey.getHandle());
    taskSizeEstimators.add(taskSizeEstimator);
    for (int threadNo = 0; threadNo < numThreads; threadNo++) {
      SenderTask senderTask;
      switch (handlerKey.getEntityType()) {
        case POINT:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.POINT,
              PUSH_FORMAT_WAVEFRONT, apiContainer.getProxyV2API(), proxyId, handlerKey.getHandle(),
              threadNo, globalRateLimiter, pushFlushInterval, pointsPerBatch, memoryBufferLimit,
              taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case DELTA_COUNTER:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.DELTA_COUNTER,
              PUSH_FORMAT_WAVEFRONT, apiContainer.getProxyV2API(), proxyId, handlerKey.getHandle(),
              threadNo, globalRateLimiter, pushFlushInterval, pointsPerBatch, memoryBufferLimit,
              taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case HISTOGRAM:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.HISTOGRAM,
              PUSH_FORMAT_HISTOGRAM, apiContainer.getProxyV2API(), proxyId, handlerKey.getHandle(),
              threadNo, globalRateLimiter, pushFlushInterval, pointsPerBatch, memoryBufferLimit,
              taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case SOURCE_TAG:
          senderTask = new ReportSourceTagSenderTask(apiContainer.getSourceTagAPI(),
              handlerKey.getHandle(), threadNo, pushFlushInterval, SOURCE_TAG_RATE_LIMITER,
              pointsPerBatch, memoryBufferLimit, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.TRACE,
              PUSH_FORMAT_TRACING, apiContainer.getProxyV2API(), proxyId, handlerKey.getHandle(),
              threadNo, globalRateLimiter, pushFlushInterval, pointsPerBatch, memoryBufferLimit,
              taskSizeEstimator, taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case TRACE_SPAN_LOGS:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.TRACE_SPAN_LOGS,
              PUSH_FORMAT_TRACING_SPAN_LOGS, apiContainer.getProxyV2API(), proxyId,
              handlerKey.getHandle(), threadNo, globalRateLimiter, pushFlushInterval,
              pointsPerBatch, memoryBufferLimit, taskSizeEstimator,
              taskQueueFactory.getTaskQueue(handlerKey, threadNo));
          break;
        case EVENT:
          senderTask = new EventSenderTask(apiContainer.getEventAPI(), proxyId,
              handlerKey.getHandle(), threadNo, pushFlushInterval, EVENT_RATE_LIMITER,
              new AtomicInteger(25), new AtomicInteger(50),
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
    for (SenderTask task : managedTasks) {
      task.drainBuffersToQueue();
    }
  }
}
