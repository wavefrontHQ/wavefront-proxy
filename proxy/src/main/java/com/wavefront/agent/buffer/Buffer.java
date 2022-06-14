package com.wavefront.agent.buffer;

import com.yammer.metrics.core.Gauge;
import java.util.List;

interface Buffer {
  void registerNewPort(String port);

  void sendMsg(String port, List<String> strPoints);

  void onMsg(String port, OnMsgFunction func);

  void onMsgBatch(String port, int batchSize, OnMsgFunction func);

  Gauge<Long> getMcGauge(String port);
}
