package com.wavefront.agent.core.senders;

import static com.wavefront.common.Utils.isWavefrontResponse;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.buffers.OnMsgDelegate;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.TimerContext;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLHandshakeException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.core.util.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SenderTask implements Runnable, OnMsgDelegate {
  private static final Logger log = LoggerFactory.getLogger(SenderTask.class.getCanonicalName());
  //      new MessageDedupingLogger(LoggerFactory.getLogger(SenderTask.class.getCanonicalName()),
  // 1000, 1);

  private final QueueInfo queue;
  private final int idx;
  private final EntityProperties properties;
  private final Buffer buffer;
  private final QueueStats queueStats;

  SenderTask(
      QueueInfo queue, int idx, EntityProperties properties, Buffer buffer, QueueStats queueStats) {
    this.queue = queue;
    this.idx = idx;
    this.properties = properties;
    this.buffer = buffer;
    this.queueStats = queueStats;
  }

  @Override
  public void run() {
    try {
      buffer.onMsgBatch(queue, idx, this);
    } catch (Throwable e) {
      log.error("error sending " + queue.getEntityType().name(), e);
    }
  }

  @Override
  public boolean checkBatchSize(int items, int bytes, int newItems, int newBytes) {
    return items + newItems <= properties.getDataPerBatch();
  }

  @Override
  public boolean checkRates(int newItems, int newBytes) {
    return properties.getRateLimiter().tryAcquire(newItems);
  }

  @Override
  public void processBatch(List<String> batch) throws SenderTaskException {
    TimerContext timer =
        Metrics.newTimer(
                new MetricName("push." + queue.getName(), "", "duration"),
                TimeUnit.MILLISECONDS,
                TimeUnit.MINUTES)
            .time();

    try (Response response = submit(batch)) {
      Metrics.newCounter(
              new TaggedMetricName(
                  "push", queue.getName() + ".http." + response.getStatus() + ".count"))
          .inc();
      queueStats.sent.inc(batch.size());
      if (response.getStatus() >= 200 && response.getStatus() < 300) {
        queueStats.delivered.inc(batch.size());
        queueStats.deliveredBytes.inc(batch.stream().mapToInt(value -> value.length()).sum());
      } else {
        queueStats.failed.inc(batch.size());
        switch (response.getStatusInfo().toEnum()) {
          case NOT_ACCEPTABLE: // CollectorApiServer RejectedExecutionException
          case REQUEST_ENTITY_TOO_LARGE: // CollectorApiServer ReportBundleTooLargeException (PPS
            // exceeded)
            properties.getRateLimiter().pause();
            break;
          case FORBIDDEN:
            log.warn(
                "["
                    + queue.getName()
                    + "] HTTP "
                    + response.getStatus()
                    + ": Please verify that '"
                    + queue.getEntityType()
                    + "' is enabled for your account!");
            break;
          case UNAUTHORIZED:
          case PROXY_AUTHENTICATION_REQUIRED:
          case REQUEST_TIMEOUT:
            if (isWavefrontResponse(response)) {
              log.warn(
                  "["
                      + queue.getName()
                      + "] HTTP "
                      + response.getStatus()
                      + " (Unregistered proxy) received while sending data to Wavefront - please verify that your token is valid and has Proxy Management permissions!");
            } else {
              log.warn(
                  "["
                      + queue.getName()
                      + "] HTTP "
                      + response.getStatus()
                      + " received while sending data to Wavefront - please verify your network/HTTP proxy settings!");
            }
            break;
        }
        if (!dropOnHTTPError(response.getStatusInfo(), batch.size())) {
          throw new SenderTaskException(
              "HTTP error: "
                  + response.getStatus()
                  + " "
                  + response.getStatusInfo().getReasonPhrase());
        }
      }
    } catch (ProcessingException ex) {
      Throwable rootCause = Throwables.getRootCause(ex);
      if (rootCause instanceof UnknownHostException) {
        log.warn(
            "["
                + queue.getName()
                + "] Error sending data to Wavefront: Unknown host "
                + rootCause.getMessage()
                + ", please check your network!");
      } else if (rootCause instanceof ConnectException
          || rootCause instanceof SocketTimeoutException) {
        log.warn(
            "["
                + queue.getName()
                + "] Error sending data to Wavefront: "
                + rootCause.getMessage()
                + ", please verify your network/HTTP proxy settings!");
      } else if (ex.getCause() instanceof SSLHandshakeException) {
        log.warn(
            "["
                + queue.getName()
                + "] Error sending data to Wavefront: "
                + ex.getCause()
                + ", please verify that your environment has up-to-date root certificates!");
      } else {
        log.warn("[" + queue.getName() + "] Error sending data to Wavefront: " + rootCause);
      }
      if (log.isDebugEnabled()) {
        log.info("Full stacktrace: ", ex);
      }
      throw new SenderTaskException(rootCause.getMessage());
    } catch (Exception ex) {
      log.warn(
          "["
              + queue.getName()
              + "] Error sending data to Wavefront: "
              + Throwables.getRootCause(ex));
      if (log.isDebugEnabled()) {
        log.info("Full stacktrace: ", ex);
      }
      throw new SenderTaskException(ex.getMessage());
    } finally {
      timer.stop();
    }
  }

  /* return true if the point need to be dropped on a specif HTTP error code */
  protected boolean dropOnHTTPError(Response.StatusType statusInfo, int batchSize) {
    return false;
  }

  protected abstract Response submit(List<String> events);
}
