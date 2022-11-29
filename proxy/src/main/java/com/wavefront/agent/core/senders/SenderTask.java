package com.wavefront.agent.core.senders;

import static com.wavefront.common.Utils.isWavefrontResponse;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.common.logger.MessageDedupingLogger;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.TimerContext;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLHandshakeException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.core.util.Throwables;

abstract class SenderTask implements Runnable {
  private static final Logger log =
      new MessageDedupingLogger(Logger.getLogger(SenderTask.class.getCanonicalName()), 1000, 1);

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
      buffer.onMsgBatch(
          queue,
          idx,
          properties.getDataPerBatch(),
          properties.getRateLimiter(),
          this::processBatch);
    } catch (Throwable e) {
      log.log(Level.SEVERE, "error sending " + queue.getEntityType().name(), e);
    }
  }

  private void processBatch(List<String> batch) throws SenderTaskException {
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
            log.warning(
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
              log.warning(
                  "["
                      + queue.getName()
                      + "] HTTP "
                      + response.getStatus()
                      + " (Unregistered proxy) received while sending data to Wavefront - please verify that your token is valid and has Proxy Management permissions!");
            } else {
              log.warning(
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
        log.warning(
            "["
                + queue.getName()
                + "] Error sending data to Wavefront: Unknown host "
                + rootCause.getMessage()
                + ", please check your network!");
      } else if (rootCause instanceof ConnectException
          || rootCause instanceof SocketTimeoutException) {
        log.warning(
            "["
                + queue.getName()
                + "] Error sending data to Wavefront: "
                + rootCause.getMessage()
                + ", please verify your network/HTTP proxy settings!");
      } else if (ex.getCause() instanceof SSLHandshakeException) {
        log.warning(
            "["
                + queue.getName()
                + "] Error sending data to Wavefront: "
                + ex.getCause()
                + ", please verify that your environment has up-to-date root certificates!");
      } else {
        log.warning("[" + queue.getName() + "] Error sending data to Wavefront: " + rootCause);
      }
      if (log.isLoggable(Level.FINE)) {
        log.log(Level.FINE, "Full stacktrace: ", ex);
      }
      throw new SenderTaskException(rootCause.getMessage());
    } catch (Exception ex) {
      log.warning(
          "["
              + queue.getName()
              + "] Error sending data to Wavefront: "
              + Throwables.getRootCause(ex));
      if (log.isLoggable(Level.FINE)) {
        log.log(Level.FINE, "Full stacktrace: ", ex);
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
