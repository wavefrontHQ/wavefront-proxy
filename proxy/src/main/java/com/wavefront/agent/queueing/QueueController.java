package com.wavefront.agent.queueing;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.TaskAction;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import javax.annotation.Nonnull;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * A queue controller (one per entity/port). Responsible for reporting queue-related metrics and
 * adjusting priority across queues.
 *
 * @param <T> submission task type
 *
 * @author vasily@wavefront.com
 */
public class QueueController<T extends DataSubmissionTask<T>> extends TimerTask implements Managed {
  private static final Logger logger =
      Logger.getLogger(QueueController.class.getCanonicalName());
  private static final Gson GSON = new Gson();

  protected final String handle;
  protected final ReportableEntityType entityType;
  protected final String entityName;
  protected final List<QueueProcessor<T>> processorTasks;
  protected final List<Long> lastProcessedTs;
  protected final Timer timer;

  private AtomicLong currentWeight = null;
  private final AtomicInteger queueSize = new AtomicInteger();
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  /**
   * TODO (VV): javadoc
   *
   * @param handle
   * @param entityType
   * @param processorTasks
   */
  public QueueController(String handle, ReportableEntityType entityType,
                         List<QueueProcessor<T>> processorTasks) {
    this.handle = handle;
    this.entityType = entityType;
    this.entityName = entityType == null ? "points" : entityType.toString();
    this.processorTasks = processorTasks;
    this.lastProcessedTs = processorTasks.stream().map(x -> x.lastProcessedTs).
        collect(Collectors.toList());
    this.timer = new Timer("timer-queuedservice-" + entityName + "-" + handle);

    Metrics.newGauge(new TaggedMetricName("buffer", "task-count", "port", handle),
        new Gauge<Integer>() {
          @Override
          public Integer value() {
            return queueSize.get();
          }
        });
  }

  @Override
  public void run() {
    // 1. grab current queue sizes (tasks count)
    queueSize.set(processorTasks.stream().mapToInt(x -> x.taskQueue.size()).sum());

    // 2. grab queue sizes (points/etc count)
    Long totalWeight = 0L;
    for (QueueProcessor<T> task : processorTasks) {
      totalWeight = task.taskQueue.weight() == null ? null : totalWeight + task.taskQueue.weight();
      if (totalWeight == null) break;
    }
    if (totalWeight != null) {
      if (currentWeight == null) {
        currentWeight = new AtomicLong();

        Metrics.newGauge(new TaggedMetricName("buffer", entityName + "-count", "port", handle),
            new Gauge<Long>() {
              @Override
              public Long value() {
                return currentWeight.get();
              }
            });
      }
      currentWeight.set(totalWeight);
    }

    // TODO (VV): grab last processed ts, adjust scheduler
  }

  @Override
  public void start() {
    if (isRunning.compareAndSet(false, true)) {
      timer.scheduleAtFixedRate(this, 1000, 1000);
    }
  }

  @Override
  public void stop() {
    if (isRunning.compareAndSet(true, false)) {
      timer.cancel();
    }
  }

  // TODO (VV): move some place more appropriate
  public static TaskAction parsePostingResponse(@Nonnull Response response, String handle) {
    try {
      if (response.getStatus() < 300) return TaskAction.NONE;
      switch (response.getStatus()) {
        case 406:
          return TaskAction.PUSHBACK;
        case 407:
        case 408:
          boolean isWavefrontResponse = false;
          // check if the HTTP 407/408 response was actually received from Wavefront - if it's a
          // JSON object containing "code" key, with value equal to the HTTP response code,
          // it's most likely from us.
          try {
            Status res = GSON.fromJson(response.readEntity(String.class), Status.class);
            if (res.code == response.getStatus()) isWavefrontResponse = true;
          } catch (Exception ex) {
            // ignore
          }
          if (isWavefrontResponse) {
            throw new RuntimeException("Response not accepted by server: " + response.getStatus() +
                " unclaimed proxy - please verify that your token is valid and has" +
                " Proxy Management permission!");
          } else {
            throw new RuntimeException("HTTP " + response.getStatus() + ": Please verify your " +
                "network/HTTP proxy settings!");
          }
        case 413:
          Metrics.newCounter(new TaggedMetricName("push", handle + ".http." +
              response.getStatus())).inc();
          return TaskAction.SPLIT;
        default:
          throw new RuntimeException("Server error: " + response.getStatus());
      }
    } finally {
      response.close();
    }
  }

  private static class Status {
    @JsonProperty
    String message;
    @JsonProperty
    int code;
  }
}
