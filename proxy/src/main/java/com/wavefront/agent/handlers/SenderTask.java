package com.wavefront.agent.handlers;

import com.wavefront.common.Managed;
import java.util.List;

public interface SenderTask extends Managed {
  int processSingleBatch(List<String> batch);
}
