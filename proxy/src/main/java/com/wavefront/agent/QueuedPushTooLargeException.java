package com.wavefront.agent;

import java.util.concurrent.RejectedExecutionException;

/**
 * @author Andrew Kao (andrew@wavefront.com)
 */
public class QueuedPushTooLargeException extends RejectedExecutionException {
  public QueuedPushTooLargeException(String message) {
    super(message);
  }
}
