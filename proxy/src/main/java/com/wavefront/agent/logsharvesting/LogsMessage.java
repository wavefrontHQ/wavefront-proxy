package com.wavefront.agent.logsharvesting;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public interface LogsMessage {
  String getLogLine();

  String hostOrDefault(String fallbackHost);
}
