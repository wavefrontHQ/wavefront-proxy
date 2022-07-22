package com.wavefront.agent.data;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;

/**
 * A serializable data submission task.
 *
 * @param <T> task type
 * @author vasily@wavefront.com
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "__CLASS")
public interface DataSubmissionTask<T extends DataSubmissionTask<T>> extends Serializable {

  /**
   * Returns a task weight.
   *
   * @return task weight
   */
  // TODO: review this, not need it
  int size();

  /**
   * Execute this task
   *
   * @return operation result
   */
  int execute();

  // TODO: implement
  //  List<T> splitTask(int minSplitSize, int maxSplitSize);
}
