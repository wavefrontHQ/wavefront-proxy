package com.wavefront.agent.core.senders;

public class SenderTaskException extends Exception {
  public SenderTaskException(String reasonPhrase) {
    super(reasonPhrase);
  }
}
