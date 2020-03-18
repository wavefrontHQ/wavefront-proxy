package com.wavefront.agent.data;

/**
 * Exception used to ignore 404s for DELETE API calls for sourceTags.
 *
 * @author vasily@wavefront.com
 */
public class IgnoreStatusCodeException extends DataSubmissionException {
  public IgnoreStatusCodeException(String message) {
    super(message);
  }
}
