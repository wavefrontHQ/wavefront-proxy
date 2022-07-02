package com.wavefront.agent.core.senders;

import java.util.List;

public interface SenderTask extends Runnable {
  int processSingleBatch(List<String> batch);
}
