package com.wavefront.agent.queueing;

import com.wavefront.agent.data.DataSubmissionTask;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 *
 *
 * @param <T>
 *
 * @author vasily@wavefront.com
 */
public interface TaskQueue<T extends DataSubmissionTask> {
  T peek();// throws IOException;

  void add(@Nonnull T t) throws IOException;

  void remove() throws IOException;

  void clear();

  int size();

  void close();

  Long weight();
}
