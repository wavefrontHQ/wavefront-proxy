package com.wavefront.agent.queueing;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.RecyclableRateLimiterFactory;
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

/**
 * A caching implementation of {@link QueueProcessorFactory}.
 *
 * @author vasily@wavefront.com
 */
public class QueueProcessorFactoryImpl implements QueueProcessorFactory {
  private final Map<HandlerKey, Map<Integer, QueueProcessor<?>>> queueProcessors = new HashMap<>();
  private final Map<HandlerKey, ScheduledExecutorService> executors = new HashMap<>();

  private final TaskQueueFactory taskQueueFactory;
  private final APIContainer apiContainer;
  private final UUID proxyId;
  private final boolean splitPushWhenRateLimited;
  private final int minSplitSize;
  private final int flushInterval;
  private final Supplier<Double> retryBackoffBaseSeconds;
  private final RecyclableRateLimiterFactory rateLimiterFactory;

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
   * @param rateLimiterFactory
   */
  public QueueProcessorFactoryImpl(APIContainer apiContainer,
                                   UUID proxyId,
                                   final TaskQueueFactory taskQueueFactory,
                                   final RecyclableRateLimiterFactory rateLimiterFactory,
                                   final boolean splitPushWhenRateLimited,
                                   final int minSplitSize,
                                   final int flushInterval,
                                   final Supplier<Double> retryBackoffBaseSeconds) {
    this.apiContainer = apiContainer;
    this.proxyId = proxyId;
    this.taskQueueFactory = taskQueueFactory;
    this.rateLimiterFactory = rateLimiterFactory;
    this.splitPushWhenRateLimited = splitPushWhenRateLimited;
    this.minSplitSize = minSplitSize;
    this.flushInterval = flushInterval;
    this.retryBackoffBaseSeconds = retryBackoffBaseSeconds;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends DataSubmissionTask<T>> QueueProcessor<T> getQueueProcessor(
      @NotNull HandlerKey handlerKey, int totalThreads, int threadNum) {
    return (QueueProcessor<T>) queueProcessors.computeIfAbsent(handlerKey, x -> new TreeMap<>()).
        computeIfAbsent(threadNum, x -> new QueueProcessor<T>(
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
                    injectMembers(apiContainer.getEventAPI(), proxyId);
              } else {
                throw new IllegalArgumentException("Unexpected submission task type: " +
                    task.getClass().getCanonicalName());
              }
            }, splitPushWhenRateLimited, minSplitSize, flushInterval, retryBackoffBaseSeconds,
            rateLimiterFactory.getRateLimiter(handlerKey)));
  }
}
