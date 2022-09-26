package com.wavefront.agent.core.senders;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.handlers.LineDelimitedUtils;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.api.ProxyV2API;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.Response;

class LineDelimitedSenderTask extends SenderTask {

  private final ProxyV2API proxyAPI;
  private final UUID proxyId;
  private final String pushFormat;

  LineDelimitedSenderTask(
      QueueInfo queue,
      int idx,
      String pushFormat,
      ProxyV2API proxyAPI,
      UUID proxyId,
      final EntityProperties properties,
      Buffer buffer,
      QueueStats queueStats) {
    super(queue, idx, properties, buffer, queueStats);
    this.pushFormat = pushFormat;
    this.proxyId = proxyId;
    this.proxyAPI = proxyAPI;
  }

  @Override
  protected Response submit(List<String> logs) {
    return proxyAPI.proxyReport(proxyId, pushFormat, LineDelimitedUtils.joinPushData(logs));
  }
}
