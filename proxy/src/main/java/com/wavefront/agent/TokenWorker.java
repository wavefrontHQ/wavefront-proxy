package com.wavefront.agent;

import com.wavefront.agent.api.APIContainer;

public interface TokenWorker {

  interface Scheduled {
    void run();
  }

  interface External {
    void setAPIContainer(APIContainer apiContainer);
  }
}
