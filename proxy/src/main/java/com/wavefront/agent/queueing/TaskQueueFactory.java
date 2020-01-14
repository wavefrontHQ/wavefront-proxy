package com.wavefront.agent.queueing;

import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.handlers.HandlerKey;

import javax.annotation.Nonnull;

/**
 * A factory for {@link TaskQueue} objects.
 *
 * @author vasily@wavefront.com.
 */
public interface TaskQueueFactory {

  /**
   * Create a task queue for a specified {@link HandlerKey} and thread number.
   *
   * @param handlerKey handler key for the {@code TaskQueue}. Usually part of the file name.
   * @param threadNum thread number. Usually part of the file name.
   *
   * @return task queue for the specified thread
   */
  <T extends DataSubmissionTask<T>> TaskQueue<T> getTaskQueue(@Nonnull HandlerKey handlerKey,
                                                              int threadNum);
}
