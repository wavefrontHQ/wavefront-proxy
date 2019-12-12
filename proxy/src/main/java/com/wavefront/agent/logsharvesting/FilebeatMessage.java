package com.wavefront.agent.logsharvesting;

import com.google.common.collect.ImmutableMap;

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
  private final Map<String, Object> messageData;
  private final Map<String, Object> beatData;
  private final String logLine;
  private Long timestampMillis = null;

  @SuppressWarnings("unchecked")
  public FilebeatMessage(Message wrapped) throws MalformedMessageException {
    this.wrapped = wrapped;
    this.messageData = this.wrapped.getData();
    this.beatData = (Map<String, Object>) this.messageData.getOrDefault("beat", ImmutableMap.of());
    if (!this.messageData.containsKey("message")) {
      throw new MalformedMessageException("No log line in message.");
    }
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
    // < 7.0: return beat.hostname
    if (this.beatData.containsKey("hostname")) {
      return (String) this.beatData.get("hostname");
    }
    // 7.0+: return host.name or agent.hostname
    if (this.messageData.containsKey("host")) {
      Map<?, ?> hostData = (Map<?, ?>) this.messageData.get("host");
      if (hostData.containsKey("name")) {
        return (String) hostData.get("name");
      }
    }
    if (this.messageData.containsKey("agent")) {
      Map<?, ?> agentData = (Map<?, ?>) this.messageData.get("agent");
      if (agentData.containsKey("hostname")) {
        return (String) agentData.get("hostname");
      }
    }
    return fallbackHost;
  }
}
