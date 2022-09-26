package com.wavefront.agent.core.senders;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.api.LogAPI;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.Response;

/**
 * This class is responsible for accumulating logs and uploading them in batches.
 *
 * @author amitw@vmware.com
 */
public class LogSenderTask extends SenderTask {
  public static final String AGENT_PREFIX = "WF-PROXY-AGENT-";

  private final LogAPI logAPI;
  private final UUID proxyId;

  /**
   * @param handlerKey handler key, that serves as an identifier of the log pipeline.
   * @param logAPI handles interaction with log systems as well as queueing.
   * @param proxyId id of the proxy.
   * @param properties container for mutable proxy settings.
   */
  LogSenderTask(
      QueueInfo handlerKey,
      int idx,
      LogAPI logAPI,
      UUID proxyId,
      EntityProperties properties,
      Buffer buffer,
      QueueStats queueStats) {
    super(handlerKey, idx, properties, buffer, queueStats);
    this.logAPI = logAPI;
    this.proxyId = proxyId;
  }

  protected Response submit(List<String> logs) {
    return logAPI.proxyLogsStr(
        AGENT_PREFIX + proxyId.toString(), "[" + String.join(",", logs) + "]");
  }
}
