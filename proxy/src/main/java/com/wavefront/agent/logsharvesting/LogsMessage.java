package com.wavefront.agent.logsharvesting;

import javax.annotation.Nullable;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public interface LogsMessage {
  @Nullable
  Long getTimestampMillis();

  String getLogLine();

  String hostOrDefault(String fallbackHost);
}
