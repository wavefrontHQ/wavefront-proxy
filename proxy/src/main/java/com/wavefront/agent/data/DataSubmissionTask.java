package com.wavefront.agent.data;

import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.data.ReportableEntityType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;

/**
 *
 * @param <T>
 */
public interface DataSubmissionTask<T extends DataSubmissionTask<T>> extends Serializable {

  int weight();

  long getCreatedMillis();

  int getAttempts();

  TaskResult execute(TaskQueueingDirective queueingLevel,
                     TaskQueue<T> taskQueue);

  void enqueue(TaskQueue<T> taskQueue) throws IOException;

  ReportableEntityType getEntityType();

  List<T> splitTask(int minSplitSize);
}
