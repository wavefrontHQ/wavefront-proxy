package com.wavefront.agent.core.buffers;

import java.util.List;

public interface OnMsgFunction {
  void run(List<String> batch) throws Exception;
}
