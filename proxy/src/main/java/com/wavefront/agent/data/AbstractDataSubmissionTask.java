package com.wavefront.agent.data;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.senders.SenderStats;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.common.logger.MessageDedupingLogger;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
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
abstract class AbstractDataSubmissionTask<T extends DataSubmissionTask<T>>
    implements DataSubmissionTask<T> {
  private static final int MAX_RETRIES = 15;
  private static final Logger debug =
      new MessageDedupingLogger(
          Logger.getLogger(AbstractDataSubmissionTask.class.getCanonicalName()), 1000, 1);
  private static final Logger log =
      Logger.getLogger(AbstractDataSubmissionTask.class.getCanonicalName());

  protected QueueInfo queue;
  protected Boolean limitRetries = null;

  protected transient Supplier<Long> timeProvider;
  private SenderStats senderStats;
  protected transient EntityProperties properties;

  /**
   * @param properties entity-specific wrapper for runtime properties.
   * @param queue port/handle
   * @param timeProvider time provider (in millis)
   */
  AbstractDataSubmissionTask(
      EntityProperties properties,
      QueueInfo queue,
      @Nullable Supplier<Long> timeProvider,
      SenderStats senderStats) {
    this.properties = properties;
    this.queue = queue;
    this.timeProvider = MoreObjects.firstNonNull(timeProvider, System::currentTimeMillis);
    this.senderStats = senderStats;
  }

  @Override
  public ReportableEntityType getEntityType() {
    return queue.getEntityType();
  }

  abstract Response doExecute() throws DataSubmissionException;

  // TODO: review returns
  public int execute() {
    // TODO: enqueuedTimeMillis can be extracted on getBatchMgs
    //    if (enqueuedTimeMillis < Long.MAX_VALUE) {
    //      if (timeSpentInQueue == null) {
    //        timeSpentInQueue =
    //            Metrics.newHistogram(
    //                new TaggedMetricName(
    //                    "buffer",
    //                    "queue-time",
    //                    "port",
    //                    handle.getPort(),
    //                    "content",
    //                    handle.getEntityType().toString()));
    //      }
    //      timeSpentInQueue.update(timeProvider.get() - enqueuedTimeMillis);
    //    }
    TimerContext timer =
        Metrics.newTimer(
                new MetricName("push." + queue.getName(), "", "duration"),
                TimeUnit.MILLISECONDS,
                TimeUnit.MINUTES)
            .time();
    try (Response response = doExecute()) {
      Metrics.newCounter(
              new TaggedMetricName(
                  "push", queue.getName() + ".http." + response.getStatus() + ".count"))
          .inc();

      senderStats.sent.inc(this.size());
      if (response.getStatus() >= 200 && response.getStatus() < 300) {
        senderStats.delivered.inc(this.size());
        return 0;
      } else {
        senderStats.failed.inc(this.size());
        return response.getStatus();
      }

      //      switch (response.getStatus()) {
      //        case 406:
      //        case 429:
      //          // TODO: pushback ?
      ////          if (enqueuedTimeMillis == Long.MAX_VALUE) {
      ////            if (properties.getTaskQueueLevel().isLessThan(TaskQueueLevel.PUSHBACK)) {
      ////              return TaskResult.RETRY_LATER;
      ////            }
      ////            return TaskResult.PERSISTED;
      ////          }
      //        case 401:
      //        case 403:
      //          log.warning(
      //              "["
      //                  + handle.getQueue()
      //                  + "] HTTP "
      //                  + response.getStatus()
      //                  + ": "
      //                  + "Please verify that \""
      //                  + handle.getEntityType()
      //                  + "\" is enabled for your account!");
      //        case 407:
      //        case 408:
      //          if (isWavefrontResponse(response)) {
      //            log.warning(
      //                "["
      //                    + handle.getQueue()
      //                    + "] HTTP "
      //                    + response.getStatus()
      //                    + " (Unregistered proxy) "
      //                    + "received while sending data to Wavefront - please verify that your
      // token is "
      //                    + "valid and has Proxy Management permissions!");
      //          } else {
      //            log.warning(
      //                "["
      //                    + handle.getQueue()
      //                    + "] HTTP "
      //                    + response.getStatus()
      //                    + " "
      //                    + "received while sending data to Wavefront - please verify your
      // network/HTTP proxy"
      //                    + " settings!");
      //          }
      //        case 413:
      //      splitTask(1, properties.getDataPerBatch())
      //              .forEach(
      //                      x ->
      //                              x.enqueue(
      //                                      enqueuedTimeMillis == Long.MAX_VALUE ?
      // QueueingReason.SPLIT : null));
      //        default:
      //      }

      // TODO: review this
    } catch (DataSubmissionException ex) {
      if (ex instanceof IgnoreStatusCodeException) {
        Metrics.newCounter(new TaggedMetricName("push", queue.getName() + ".http.404.count")).inc();
        Metrics.newCounter(new MetricName(queue.getName(), "", "delivered")).inc(this.size());
      }
      throw new RuntimeException("Unhandled DataSubmissionException", ex);
    } catch (ProcessingException ex) {
      Throwable rootCause = Throwables.getRootCause(ex);
      if (rootCause instanceof UnknownHostException) {
        debug.warning(
            "["
                + queue.getName()
                + "] Error sending data to Wavefront: Unknown host "
                + rootCause.getMessage()
                + ", please check your network!");
      } else if (rootCause instanceof ConnectException
          || rootCause instanceof SocketTimeoutException) {
        debug.warning(
            "["
                + queue.getName()
                + "] Error sending data to Wavefront: "
                + rootCause.getMessage()
                + ", please verify your network/HTTP proxy settings!");
      } else if (ex.getCause() instanceof SSLHandshakeException) {
        debug.warning(
            "["
                + queue.getName()
                + "] Error sending data to Wavefront: "
                + ex.getCause()
                + ", please verify that your environment has up-to-date root certificates!");
      } else {
        debug.warning("[" + queue.getName() + "] Error sending data to Wavefront: " + rootCause);
      }
      if (debug.isLoggable(Level.FINE)) {
        debug.log(Level.FINE, "Full stacktrace: ", ex);
      }
    } catch (Exception ex) {
      debug.warning(
          "["
              + queue.getName()
              + "] Error sending data to Wavefront: "
              + Throwables.getRootCause(ex));
      if (debug.isLoggable(Level.FINE)) {
        debug.log(Level.FINE, "Full stacktrace: ", ex);
      }
    } finally {
      timer.stop();
    }
    return -999;
  }
}
