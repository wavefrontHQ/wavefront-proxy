package com.wavefront.agent.queueing;

import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;

import javax.validation.constraints.NotNull;

/**
 *
 * @author vasily@wavefront.com
 */
public interface QueueProcessorFactory {
  <T extends DataSubmissionTask<T>> QueueProcessor<T> getQueueProcessor(
      @NotNull HandlerKey handlerKey, int totalThreads, int threadNum);
}
