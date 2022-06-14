package com.wavefront.agent.buffer;

import java.util.List;

public interface OnMsgFunction {
  void run(List<String> batch) throws Exception;
}
