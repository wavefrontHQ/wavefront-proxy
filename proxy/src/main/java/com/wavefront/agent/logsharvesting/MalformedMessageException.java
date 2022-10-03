package com.wavefront.agent.logsharvesting;

public class MalformedMessageException extends Exception {
  MalformedMessageException(String msg) {
    super(msg);
  }
}
