package com.wavefront.agent.logsharvesting;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class MalformedMessageException extends Exception {
  MalformedMessageException(String msg) {
    super(msg);
  }
}
