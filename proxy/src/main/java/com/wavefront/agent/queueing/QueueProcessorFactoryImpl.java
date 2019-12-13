package com.wavefront.agent.queueing;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.config.ProxyRuntimeProperties;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.RecyclableRateLimiterFactory;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * A caching implementation of {@link QueueProcessorFactory}.
 *
 * @author vasily@wavefront.com
 */
public class QueueProcessorFactoryImpl implements QueueProcessorFactory {

  private final Map<HandlerKey, Map<Integer, QueueProcessor<?>>> queueProcessors = new HashMap<>();
  private final TaskQueueFactory taskQueueFactory;
  private final APIContainer apiContainer;
  private final UUID proxyId;
  private final RecyclableRateLimiterFactory rateLimiterFactory;
  private final ProxyRuntimeProperties runtimeProperties;

  /**
   * TODO (VV): javadoc
   *
   * @param apiContainer
   * @param proxyId
   * @param taskQueueFactory
   * @param rateLimiterFactory
   * @param runtimeProperties
   */
  public QueueProcessorFactoryImpl(APIContainer apiContainer,
                                   UUID proxyId,
                                   final TaskQueueFactory taskQueueFactory,
                                   final RecyclableRateLimiterFactory rateLimiterFactory,
                                   final ProxyRuntimeProperties runtimeProperties) {
    this.apiContainer = apiContainer;
    this.proxyId = proxyId;
    this.taskQueueFactory = taskQueueFactory;
    this.rateLimiterFactory = rateLimiterFactory;
    this.runtimeProperties = runtimeProperties;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends DataSubmissionTask<T>> QueueProcessor<T> getQueueProcessor(
      @NotNull HandlerKey handlerKey, int threadNum) {
    return (QueueProcessor<T>) queueProcessors.computeIfAbsent(handlerKey, x -> new TreeMap<>()).
        computeIfAbsent(threadNum, x -> new QueueProcessor<T>(handlerKey, threadNum,
            taskQueueFactory.getTaskQueue(handlerKey, threadNum),
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
            }, runtimeProperties, rateLimiterFactory.getRateLimiter(handlerKey)));
  }
}
