package org.logstash.beats;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

/**
 * Identity of a filebeat batch, based on the first message. Used for duplicate batch detection.
 *
 * @author vasily@wavefront.com.
 */
public class BatchIdentity {
  private final String timestampStr;
  private final int highestSequence;
  private final int size;
  @Nullable
  private final String logFile;
  @Nullable
  private final Integer logFileOffset;

  BatchIdentity(String timestampStr, int highestSequence, int size, @Nullable String logFile,
                @Nullable Integer logFileOffset) {
    this.timestampStr = timestampStr;
    this.highestSequence = highestSequence;
    this.size = size;
    this.logFile = logFile;
    this.logFileOffset = logFileOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BatchIdentity that = (BatchIdentity) o;
    return this.highestSequence == that.highestSequence &&
        this.size == that.size &&
        Objects.equals(this.timestampStr, that.timestampStr) &&
        Objects.equals(this.logFile, that.logFile) &&
        Objects.equals(this.logFileOffset, that.logFileOffset);
  }

  @Override
  public int hashCode() {
    int result = timestampStr != null ? timestampStr.hashCode() : 0;
    result = 31 * result + highestSequence;
    result = 31 * result + size;
    result = 31 * result + (logFile != null ? logFile.hashCode() : 0);
    result = 31 * result + (logFileOffset != null ? logFileOffset.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "BatchIdentity{timestampStr=" + timestampStr +
        ", highestSequence=" + highestSequence +
        ", size=" + size +
        ", logFile=" + logFile +
        ", logFileOffset=" + logFileOffset +
        "}";
  }

  @Nullable
  public static BatchIdentity valueFrom(Message message) {
    Map messageData = message.getData();
    if (!messageData.containsKey("@timestamp")) return null;
    String logFile = null;
    Integer logFileOffset = null;
    if (messageData.containsKey("log")) {
      Map logData = (Map) messageData.get("log");
      if (logData.containsKey("offset") && logData.containsKey("file")) {
        Map logFileData = (Map) logData.get("file");
        if (logFileData.containsKey("path")) {
          logFile = (String) logFileData.get("path");
          logFileOffset = (Integer) logData.get("offset");
        }
      }
    }
    return new BatchIdentity((String) messageData.get("@timestamp"),
        message.getBatch().getHighestSequence(), message.getBatch().size(), logFile, logFileOffset);
  }

  @Nullable
  public static String keyFrom(Message message) {
    Map messageData = message.getData();
    if (messageData.containsKey("agent")) {
      Map agentData = (Map) messageData.get("agent");
      if (agentData.containsKey("id")) {
        return (String) agentData.get("id");
      }
    }
    if (messageData.containsKey("host")) {
      Map hostData = (Map) messageData.get("host");
      if (hostData.containsKey("name")) {
        return (String) hostData.get("name");
      }
    }
    return null;
  }
}
