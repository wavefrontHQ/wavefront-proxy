package com.wavefront.agent.core.buffers;

import java.util.List;

public interface OnMsgDelegate {
  void processBatch(List<String> batch) throws Exception;

  boolean checkBatchSize(int items, int bytes, int newItems, int newBytes);

  boolean checkRates(int newItems, int newBytes);
}
