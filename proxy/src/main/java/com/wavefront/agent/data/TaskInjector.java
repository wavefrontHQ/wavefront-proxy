package com.wavefront.agent.data;

/**
 * Class to inject non-serializable members into a {@link DataSubmissionTask} before execution
 *
 * @author vasily@wavefront.com
 */
public interface TaskInjector<T extends DataSubmissionTask<T>> {

  /**
   * Inject members into specified task.
   *
   * @param task task to inject
   */
  void inject(T task);
}
