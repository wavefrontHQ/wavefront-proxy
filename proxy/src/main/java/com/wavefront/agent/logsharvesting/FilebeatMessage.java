package com.wavefront.agent.logsharvesting;

import org.logstash.beats.Message;

import java.util.Map;

/**
 * Abstraction for {@link org.logstash.beats.Message}
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FilebeatMessage {
  private final Message wrapped;
  private final Map messageData;
  private final Map beatData;
  private final String logLine;

  public FilebeatMessage(Message wrapped) throws MalformedMessageException {
    this.wrapped = wrapped;
    this.messageData = this.wrapped.getData();
    if (!this.messageData.containsKey("beat")) throw new MalformedMessageException("No beat metadata.");
    this.beatData = (Map) this.messageData.get("beat");
    if (!this.messageData.containsKey("message")) throw new MalformedMessageException("No log line in message.");
    this.logLine = (String) this.messageData.get("message");
  }

  public String getLogLine() {
    return logLine;
  }

  public String hostOrDefault(String fallbackHost) {
    if (this.beatData.containsKey("hostname")) {
      return (String) this.beatData.get("hostname");
    }
    return fallbackHost;
  }
}
