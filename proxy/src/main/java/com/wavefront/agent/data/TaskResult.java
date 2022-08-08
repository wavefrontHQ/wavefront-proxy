package com.wavefront.agent.data;

/**
 * Possible outcomes of {@link DataSubmissionTask} execution
 *
 * @author vasily@wavefront.com
 */
public enum TaskResult {
  DELIVERED, // success
  REMOVED, // data is removed from queue, due to feature disabled or auth error
  PERSISTED, // data is persisted in the queue, start back-off process
  PERSISTED_RETRY, // data is persisted in the queue, ok to continue processing backlog
  RETRY_LATER // data needs to be returned to the pool and retried later
}
