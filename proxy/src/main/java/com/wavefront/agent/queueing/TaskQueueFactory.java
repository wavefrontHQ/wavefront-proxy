package com.wavefront.agent.queueing;

import com.wavefront.agent.handlers.HandlerKey;

import javax.validation.constraints.NotNull;

/**
 *
 *
 * @author vasily@wavefront.com.
 */
public interface TaskQueueFactory {

  TaskQueue getTaskQueue(@NotNull HandlerKey handlerKey, int threadNum);
}
