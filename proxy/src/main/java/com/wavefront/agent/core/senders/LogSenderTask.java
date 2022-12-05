package com.wavefront.agent.core.senders;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.api.LogAPI;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is responsible for accumulating logs and uploading them in batches. */
public class LogSenderTask extends SenderTask {
  private static final Logger LOGGER = LoggerFactory.getLogger("LogDataSubmission");
  public static final String AGENT_PREFIX = "WF-PROXY-AGENT-";

  private final QueueInfo queue;
  private final LogAPI logAPI;
  private final UUID proxyId;

  /**
   * @param queue handler key, that serves as an identifier of the log pipeline.
   * @param logAPI handles interaction with log systems as well as queueing.
   * @param proxyId id of the proxy.
   * @param properties container for mutable proxy settings.
   */
  LogSenderTask(
      QueueInfo queue,
      int idx,
      LogAPI logAPI,
      UUID proxyId,
      EntityProperties properties,
      Buffer buffer,
      QueueStats queueStats) {
    super(queue, idx, properties, buffer, queueStats);
    this.queue = queue;
    this.logAPI = logAPI;
    this.proxyId = proxyId;
  }

  protected Response submit(List<String> logs) {
    if (LOGGER.isDebugEnabled()) {
      for (String log : logs) {
        LOGGER.debug("Sending a log to the backend: " + log);
      }
    }
    return logAPI.proxyLogsStr(
        AGENT_PREFIX + proxyId.toString(), "[" + String.join(",", logs) + "]");
  }

  // A 429 from VRLIC means that the daily ingestion limit has been reached
  @Override
  protected boolean dropOnHTTPError(Response.StatusType statusInfo, int batchSize) {
    if (statusInfo.getStatusCode() == 429) {
      Metrics.newCounter(new MetricName(queue.getName(), "", "failed" + ".ingestion_limit_reached"))
          .inc(batchSize);
      return true;
    }
    return super.dropOnHTTPError(statusInfo, batchSize);
  }
}
