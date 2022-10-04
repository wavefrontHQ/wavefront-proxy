package com.wavefront.agent.logsharvesting;

public interface LogsMessage {
  String getLogLine();

  String hostOrDefault(String fallbackHost);
}
