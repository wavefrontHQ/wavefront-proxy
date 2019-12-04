package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RecyclableRateLimiter;

import com.google.common.util.concurrent.RecyclableRateLimiterImpl;
import com.wavefront.agent.api.ForceQueueEnabledProxyAPI;
import com.wavefront.data.ReportableEntityType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

  private final ForceQueueEnabledProxyAPI proxyAPI;
  private final UUID proxyId;
  private final RecyclableRateLimiter globalRateLimiter;
  private final AtomicInteger pushFlushInterval;
  private final AtomicInteger pointsPerBatch;
  private final AtomicInteger memoryBufferLimit;

  // TODO: sync with backend
  private static final RecyclableRateLimiter SOURCE_TAG_RATE_LIMITER =
      RecyclableRateLimiterImpl.create(5, 10);
  private static final RecyclableRateLimiter EVENT_RATE_LIMITER =
      RecyclableRateLimiterImpl.create(5, 10);

  /**
   * Create new instance.
   *
   * @param proxyAPI          handles interaction with Wavefront servers as well as queueing.
   * @param proxyId           proxy ID.
   * @param globalRateLimiter rate limiter to control outbound point rate.
   * @param pushFlushInterval interval between flushes.
   * @param itemsPerBatch     max points per flush.
   * @param memoryBufferLimit max points in task's memory buffer before queueing.
   */
  public SenderTaskFactoryImpl(final ForceQueueEnabledProxyAPI proxyAPI,
                               final UUID proxyId,
                               final RecyclableRateLimiter globalRateLimiter,
                               final AtomicInteger pushFlushInterval,
                               @Nullable final AtomicInteger itemsPerBatch,
                               @Nullable final AtomicInteger memoryBufferLimit) {
    this.proxyAPI = proxyAPI;
    this.proxyId = proxyId;
    this.globalRateLimiter = globalRateLimiter;
    this.pushFlushInterval = pushFlushInterval;
    this.pointsPerBatch = itemsPerBatch;
    this.memoryBufferLimit = memoryBufferLimit;
  }

  public Collection<SenderTask> createSenderTasks(@NotNull HandlerKey handlerKey,
                                                  final int numThreads) {
    List<SenderTask> toReturn = new ArrayList<>(numThreads);
    for (int threadNo = 0; threadNo < numThreads; threadNo++) {
      SenderTask senderTask;
      switch (handlerKey.getEntityType()) {
        case POINT:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.POINT.toString(),
              PUSH_FORMAT_WAVEFRONT, proxyAPI, proxyId, handlerKey.getHandle(), threadNo,
              globalRateLimiter, pushFlushInterval, pointsPerBatch, memoryBufferLimit);
          break;
        case DELTA_COUNTER:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.DELTA_COUNTER.toString(),
              PUSH_FORMAT_WAVEFRONT, proxyAPI, proxyId, handlerKey.getHandle(), threadNo,
              globalRateLimiter, pushFlushInterval, pointsPerBatch, memoryBufferLimit);
          break;
        case HISTOGRAM:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.HISTOGRAM.toString(),
              PUSH_FORMAT_HISTOGRAM, proxyAPI, proxyId, handlerKey.getHandle(), threadNo,
              globalRateLimiter, pushFlushInterval, pointsPerBatch, memoryBufferLimit);
          break;
        case SOURCE_TAG:
          senderTask = new ReportSourceTagSenderTask(proxyAPI, handlerKey.getHandle(),
              threadNo, pushFlushInterval, SOURCE_TAG_RATE_LIMITER, pointsPerBatch, memoryBufferLimit);
          break;
        case TRACE:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.TRACE.toString(),
              PUSH_FORMAT_TRACING, proxyAPI, proxyId, handlerKey.getHandle(), threadNo,
              globalRateLimiter, pushFlushInterval, pointsPerBatch, memoryBufferLimit);
          break;
        case TRACE_SPAN_LOGS:
          senderTask = new LineDelimitedSenderTask(ReportableEntityType.TRACE_SPAN_LOGS.toString(),
              PUSH_FORMAT_TRACING_SPAN_LOGS, proxyAPI, proxyId, handlerKey.getHandle(), threadNo,
              globalRateLimiter, pushFlushInterval, pointsPerBatch, memoryBufferLimit);
          break;
        case EVENT:
          senderTask = new EventSenderTask(proxyAPI, proxyId, handlerKey.getHandle(), threadNo,
              pushFlushInterval, EVENT_RATE_LIMITER, pointsPerBatch, memoryBufferLimit);
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
