package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.wavefront.data.ReportableEntityType;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

/**
 * A serializable data submission task.
 *
 * @param <T> task type
 *
 * @author vasily@wavefront.com
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "__CLASS")
public interface DataSubmissionTask<T extends DataSubmissionTask<T>> extends Serializable {

  /**
   * Returns a task weight.
   *
   * @return task weight
   */
  int weight();

  /**
   * Returns task enqueue time in milliseconds.
   *
   * @return enqueue time in milliseconds
   */
  long getEnqueuedMillis();

  /**
   * Execute this task
   *
   * @return operation result
   */
  TaskResult execute();

  /**
   * Persist task in the queue
   *
   * @param reason reason for queueing. used to increment metrics, if specified.
   */
  void enqueue(@Nullable QueueingReason reason);

  /**
   * Returns entity type handled.
   *
   * @return entity type
   */
  ReportableEntityType getEntityType();

  /**
   * Split the task into smaller tasks.
   *
   * @param minSplitSize Don't split the task if its weight is smaller than this number.
   * @param maxSplitSize Split tasks size cap.
   * @return tasks
   */
  List<T> splitTask(int minSplitSize, int maxSplitSize);
}
