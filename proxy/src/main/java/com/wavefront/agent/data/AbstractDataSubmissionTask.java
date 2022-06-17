package com.wavefront.agent.data;

import static com.wavefront.common.Utils.isWavefrontResponse;
import static java.lang.Boolean.TRUE;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.common.logger.MessageDedupingLogger;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.TimerContext;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.net.ssl.SSLHandshakeException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.Response;

/**
 * A base class for data submission tasks.
 *
 * @param <T> task type
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
abstract class AbstractDataSubmissionTask<T extends DataSubmissionTask<T>>
    implements DataSubmissionTask<T> {
  private static final int MAX_RETRIES = 15;
  private static final Logger log =
      new MessageDedupingLogger(
          Logger.getLogger(AbstractDataSubmissionTask.class.getCanonicalName()), 1000, 1);

  @JsonProperty protected long enqueuedTimeMillis = Long.MAX_VALUE;
  @JsonProperty protected int attempts = 0;
  @JsonProperty protected int serverErrors = 0;
  @JsonProperty protected HandlerKey handle;
  @JsonProperty protected Boolean limitRetries = null;

  protected transient Histogram timeSpentInQueue;
  protected transient Supplier<Long> timeProvider;
  protected transient EntityProperties properties;

  AbstractDataSubmissionTask() {}

  /**
   * @param properties entity-specific wrapper for runtime properties.
   * @param handle port/handle
   * @param timeProvider time provider (in millis)
   */
  AbstractDataSubmissionTask(
      EntityProperties properties, HandlerKey handle, @Nullable Supplier<Long> timeProvider) {
    this.properties = properties;
    this.handle = handle;
    this.timeProvider = MoreObjects.firstNonNull(timeProvider, System::currentTimeMillis);
  }

  @Override
  public long getEnqueuedMillis() {
    return enqueuedTimeMillis;
  }

  @Override
  public ReportableEntityType getEntityType() {
    return handle.getEntityType();
  }

  abstract Response doExecute() throws DataSubmissionException;

  // TODO: review returns
  public TaskResult execute() {
    if (enqueuedTimeMillis < Long.MAX_VALUE) {
      if (timeSpentInQueue == null) {
        timeSpentInQueue =
            Metrics.newHistogram(
                new TaggedMetricName(
                    "buffer",
                    "queue-time",
                    "port",
                    handle.getPort(),
                    "content",
                    handle.getEntityType().toString()));
      }
      timeSpentInQueue.update(timeProvider.get() - enqueuedTimeMillis);
    }
    attempts += 1;
    TimerContext timer =
        Metrics.newTimer(
                new MetricName("push." + handle.getQueue(), "", "duration"),
                TimeUnit.MILLISECONDS,
                TimeUnit.MINUTES)
            .time();
    try (Response response = doExecute()) {
      Metrics.newCounter(
              new TaggedMetricName(
                  "push", handle.getQueue() + ".http." + response.getStatus() + ".count"))
          .inc();
      if (response.getStatus() >= 200 && response.getStatus() < 300) {
        Metrics.newCounter(new MetricName(handle.getQueue(), "", "delivered")).inc(this.weight());
        return TaskResult.DELIVERED;
      }
      switch (response.getStatus()) {
        case 406:
        case 429:
          if (enqueuedTimeMillis == Long.MAX_VALUE) {
            if (properties.getTaskQueueLevel().isLessThan(TaskQueueLevel.PUSHBACK)) {
              return TaskResult.RETRY_LATER;
            }
            return TaskResult.PERSISTED;
          }
          return TaskResult.RETRY_LATER;
        case 401:
        case 403:
          log.warning(
              "["
                  + handle.getQueue()
                  + "] HTTP "
                  + response.getStatus()
                  + ": "
                  + "Please verify that \""
                  + handle.getEntityType()
                  + "\" is enabled for your account!");
        case 407:
        case 408:
          if (isWavefrontResponse(response)) {
            log.warning(
                "["
                    + handle.getQueue()
                    + "] HTTP "
                    + response.getStatus()
                    + " (Unregistered proxy) "
                    + "received while sending data to Wavefront - please verify that your token is "
                    + "valid and has Proxy Management permissions!");
          } else {
            log.warning(
                "["
                    + handle.getQueue()
                    + "] HTTP "
                    + response.getStatus()
                    + " "
                    + "received while sending data to Wavefront - please verify your network/HTTP proxy"
                    + " settings!");
          }
          return TaskResult.RETRY_LATER;
        case 413:
          return TaskResult.PERSISTED_RETRY;
        default:
          serverErrors += 1;
          if (serverErrors > MAX_RETRIES && TRUE.equals(limitRetries)) {
            log.info(
                "["
                    + handle.getQueue()
                    + "] HTTP "
                    + response.getStatus()
                    + " received while sending "
                    + "data to Wavefront, max retries reached");
            return TaskResult.DELIVERED;
          } else {
            log.info(
                "["
                    + handle.getQueue()
                    + "] HTTP "
                    + response.getStatus()
                    + " received while sending "
                    + "data to Wavefront, retrying");
            return TaskResult.PERSISTED_RETRY;
          }
      }
    } catch (DataSubmissionException ex) {
      if (ex instanceof IgnoreStatusCodeException) {
        Metrics.newCounter(new TaggedMetricName("push", handle.getQueue() + ".http.404.count"))
            .inc();
        Metrics.newCounter(new MetricName(handle.getQueue(), "", "delivered")).inc(this.weight());
        return TaskResult.DELIVERED;
      }
      throw new RuntimeException("Unhandled DataSubmissionException", ex);
    } catch (ProcessingException ex) {
      Throwable rootCause = Throwables.getRootCause(ex);
      if (rootCause instanceof UnknownHostException) {
        log.warning(
            "["
                + handle.getQueue()
                + "] Error sending data to Wavefront: Unknown host "
                + rootCause.getMessage()
                + ", please check your network!");
      } else if (rootCause instanceof ConnectException
          || rootCause instanceof SocketTimeoutException) {
        log.warning(
            "["
                + handle.getQueue()
                + "] Error sending data to Wavefront: "
                + rootCause.getMessage()
                + ", please verify your network/HTTP proxy settings!");
      } else if (ex.getCause() instanceof SSLHandshakeException) {
        log.warning(
            "["
                + handle.getQueue()
                + "] Error sending data to Wavefront: "
                + ex.getCause()
                + ", please verify that your environment has up-to-date root certificates!");
      } else {
        log.warning("[" + handle.getQueue() + "] Error sending data to Wavefront: " + rootCause);
      }
      if (log.isLoggable(Level.FINE)) {
        log.log(Level.FINE, "Full stacktrace: ", ex);
      }
      return TaskResult.PERSISTED_RETRY;
    } catch (Exception ex) {
      log.warning(
          "["
              + handle.getQueue()
              + "] Error sending data to Wavefront: "
              + Throwables.getRootCause(ex));
      if (log.isLoggable(Level.FINE)) {
        log.log(Level.FINE, "Full stacktrace: ", ex);
      }
      return TaskResult.PERSISTED_RETRY;
    } finally {
      timer.stop();
    }
  }
}
