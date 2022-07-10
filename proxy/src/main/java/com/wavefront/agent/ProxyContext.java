package com.wavefront.agent;

import com.wavefront.agent.core.queues.QueuesManager;

// This class is for storing things that are used all over the Proxy and need to ve override on test
// in the future we need to use @inject or something similar

public class ProxyContext {
  public static QueuesManager queuesManager;
}
