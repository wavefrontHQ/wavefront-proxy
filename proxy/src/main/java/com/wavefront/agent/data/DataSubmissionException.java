package com.wavefront.agent.data;

/**
 * Exception to bypass standard handling for response status codes.

 * @author vasily@wavefront.com
 */
public abstract class DataSubmissionException extends Exception {
  public DataSubmissionException(String message) {
    super(message);
  }
}
