package com.wavefront.agent.core.senders;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.api.EventAPI;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.Response;

class EventSenderTask extends SenderTask {
  private final EventAPI proxyAPI;
  private final UUID proxyId;

  /**
   * @param queue handler key, that serves as an identifier of the metrics pipeline.
   * @param proxyAPI handles interaction with Wavefront servers as well as queueing.
   * @param proxyId id of the proxy.
   * @param properties container for mutable proxy settings.
   */
  EventSenderTask(
      QueueInfo queue,
      int idx,
      EventAPI proxyAPI,
      UUID proxyId,
      EntityProperties properties,
      Buffer buffer,
      QueueStats queueStats) {
    super(queue, idx, properties, buffer, queueStats);
    this.proxyAPI = proxyAPI;
    this.proxyId = proxyId;
  }

  public Response submit(List<String> events) {
    return proxyAPI.proxyEventsString(proxyId, "[" + String.join(",", events) + "]");
  }
}
