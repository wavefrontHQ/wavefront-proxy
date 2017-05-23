package com.wavefront.agent.logsharvesting;

import org.logstash.beats.Message;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Abstraction for {@link org.logstash.beats.Message}
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FilebeatMessage implements LogsMessage {
  private final Message wrapped;
  private final Map messageData;
  private final Map beatData;
  private final String logLine;
  private Long timestampMillis = null;

  public FilebeatMessage(Message wrapped) throws MalformedMessageException {
    this.wrapped = wrapped;
    this.messageData = this.wrapped.getData();
    if (!this.messageData.containsKey("beat")) throw new MalformedMessageException("No beat metadata.");
    this.beatData = (Map) this.messageData.get("beat");
    if (!this.messageData.containsKey("message")) throw new MalformedMessageException("No log line in message.");
    this.logLine = (String) this.messageData.get("message");
    if (getTimestampMillis() == null) throw new MalformedMessageException("No timestamp metadata.");
  }

  @Nullable
  public Long getTimestampMillis() {
    if (timestampMillis == null) {
      if (!this.messageData.containsKey("@timestamp")) return null;
      String timestampStr = (String) this.messageData.get("@timestamp");
      try {
        TemporalAccessor accessor = DateTimeFormatter.ISO_DATE_TIME.parse(timestampStr);
        timestampMillis = Date.from(Instant.from(accessor)).getTime();
      } catch (DateTimeParseException e) {
        return null;
      }
    }
    return timestampMillis;
  }

  @Override
  public String getLogLine() {
    return logLine;
  }

  @Override
  public String hostOrDefault(String fallbackHost) {
    if (this.beatData.containsKey("hostname")) {
      return (String) this.beatData.get("hostname");
    }
    return fallbackHost;
  }
}
