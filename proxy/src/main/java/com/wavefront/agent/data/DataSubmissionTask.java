package com.wavefront.agent.data;

import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.data.ReportableEntityType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * A serializable data submission task.
 *
 * @param <T>
 */
public interface DataSubmissionTask<T extends DataSubmissionTask<T>> extends Serializable {

  /**
   * Returns a task weight.
   *
   * @return task weight
   */
  int weight();

  /**
   * Returns task creation time in milliseconds.
   *
   * @return created time in milliseconds
   */
  long getCreatedMillis();

  /**
   * Returns the number of attempts made while executing this task.
   *
   * @return number of attempts
   */
  int getAttempts();

  /**
   * Execute this task
   *
   * @param queueingLevel
   * @param taskQueue
   *
   * @return operation result
   */
  TaskResult execute(TaskQueueingDirective queueingLevel,
                     TaskQueue<T> taskQueue);

  /**
   * Persist task in the queue
   *
   * @param taskQueue queue to use
   *
   * @throws IOException
   */
  void enqueue(TaskQueue<T> taskQueue) throws IOException;

  /**
   * Returns entity type handled.
   *
   * @return entity type
   */
  ReportableEntityType getEntityType();

  /**
   * Split the task into two smaller tasks.
   *
   * @param minSplitSize Don't split the task if its weight is smaller than this number.
   * @return tasks
   */
  List<T> splitTask(int minSplitSize);
}
