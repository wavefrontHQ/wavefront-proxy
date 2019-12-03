package com.wavefront.agent.queueing;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.common.NamedThreadFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.wavefront.agent.handlers.SenderTaskFactoryImpl.EVENT_RATE_LIMITER;
import static com.wavefront.agent.handlers.SenderTaskFactoryImpl.SOURCE_TAG_RATE_LIMITER;

/**
 * A caching implementation of {@link QueueProcessorFactory}.
 *
 * @author vasily@wavefront.com
 */
public class QueueProcessorFactoryImpl implements QueueProcessorFactory {
  private final Map<HandlerKey, Map<Integer, QueueProcessor>> queueProcessors = new HashMap<>();
  private final Map<HandlerKey, ScheduledExecutorService> executors = new HashMap<>();

  private final TaskQueueFactory taskQueueFactory;
  private final APIContainer apiContainer;
  private final UUID proxyId;
  private final boolean splitPushWhenRateLimited;
  private final int minSplitSize;
  private final int flushInterval;
  private final Supplier<Double> retryBackoffBaseSeconds;
  private final RecyclableRateLimiter pushRateLimiter;
  private final RecyclableRateLimiter pushRateLimiterHistogram;
  private final RecyclableRateLimiter pushRateLimiterSpans;
  private final RecyclableRateLimiter pushRateLimiterSpanLogs;

  /**
   * Creates a new instance.
   *
   * @param taskQueueFactory
   * @param apiContainer
   * @param proxyId
   * @param splitPushWhenRateLimited
   * @param minSplitSize
   * @param flushInterval
   * @param retryBackoffBaseSeconds
   * @param pushRateLimiter
   * @param pushRateLimiterHistograms
   * @param pushRateLimiterSpans
   * @param pushRateLimiterSpanLogs
   */
  public QueueProcessorFactoryImpl(TaskQueueFactory taskQueueFactory,
                                   APIContainer apiContainer,
                                   UUID proxyId,
                                   final boolean splitPushWhenRateLimited,
                                   final int minSplitSize,
                                   final int flushInterval,
                                   final Supplier<Double> retryBackoffBaseSeconds,
                                   @Nullable final RecyclableRateLimiter pushRateLimiter,
                                   @Nullable final RecyclableRateLimiter pushRateLimiterHistograms,
                                   @Nullable final RecyclableRateLimiter pushRateLimiterSpans,
                                   @Nullable final RecyclableRateLimiter pushRateLimiterSpanLogs) {
    this.taskQueueFactory = taskQueueFactory;
    this.apiContainer = apiContainer;
    this.proxyId = proxyId;
    this.splitPushWhenRateLimited = splitPushWhenRateLimited;
    this.minSplitSize = minSplitSize;
    this.flushInterval = flushInterval;
    this.retryBackoffBaseSeconds = retryBackoffBaseSeconds;
    this.pushRateLimiter = pushRateLimiter;
    this.pushRateLimiterHistogram = pushRateLimiterHistograms;
    this.pushRateLimiterSpans = pushRateLimiterSpans;
    this.pushRateLimiterSpanLogs = pushRateLimiterSpanLogs;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends DataSubmissionTask<T>> QueueProcessor<T> getQueueProcessor(
      @NotNull HandlerKey handlerKey, int totalThreads, int threadNum) {
    return queueProcessors.computeIfAbsent(handlerKey, x -> new TreeMap<>()).
        computeIfAbsent(threadNum, x -> {
          RecyclableRateLimiter rateLimiter;
          switch (handlerKey.getEntityType()) {
            case POINT:
              rateLimiter = pushRateLimiter;
              break;
            case HISTOGRAM:
              rateLimiter = pushRateLimiterHistogram;
              break;
            case TRACE:
              rateLimiter = pushRateLimiterSpans;
              break;
            case TRACE_SPAN_LOGS:
              rateLimiter = pushRateLimiterSpanLogs;
              break;
            case SOURCE_TAG:
              rateLimiter = SOURCE_TAG_RATE_LIMITER;
              break;
            case EVENT:
              rateLimiter = EVENT_RATE_LIMITER;
              break;
            default:
              throw new IllegalArgumentException("Unexpected entity type " +
                  handlerKey.getEntityType().name() + " for " + handlerKey.getHandle());
          }
          return new QueueProcessor<T>(
              handlerKey.getHandle(), taskQueueFactory.getTaskQueue(handlerKey, threadNum),
              executors.computeIfAbsent(handlerKey,
                  executor -> Executors.newScheduledThreadPool(totalThreads,
                      new NamedThreadFactory("queueprocessor-" + handlerKey.getEntityType() + "-" +
                          handlerKey.getHandle()))),
              task -> {
                if (task instanceof LineDelimitedDataSubmissionTask) {
                  ((LineDelimitedDataSubmissionTask) task).
                      injectMembers(apiContainer.getProxyV2API(), proxyId);
                } else if (task instanceof SourceTagSubmissionTask) {
                  ((SourceTagSubmissionTask) task).
                      injectMembers(apiContainer.getSourceTagAPI());
                } else if (task instanceof EventDataSubmissionTask) {
                  ((EventDataSubmissionTask) task).
                      injectMembers(apiContainer.getEventAPI());
                } else {
                  throw new IllegalArgumentException("Unexpected submission task type: " +
                      task.getClass().getCanonicalName());
                }
              }, splitPushWhenRateLimited, minSplitSize, flushInterval, retryBackoffBaseSeconds,
              rateLimiter);
        });
  }
}
