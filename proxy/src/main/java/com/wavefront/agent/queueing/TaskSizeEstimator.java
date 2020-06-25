package com.wavefront.agent.queueing;

import com.google.common.util.concurrent.RateLimiter;
import com.wavefront.agent.SharedMetricsRegistry;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricsRegistry;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Calculates approximate task sizes to estimate how quickly we would run out of disk space
 * if we are no longer able to send data to the server endpoint (i.e. network outage).
 *
 * @author vasily@wavefront.com.
 */
public class TaskSizeEstimator {
  private static final MetricsRegistry REGISTRY = SharedMetricsRegistry.getInstance();
  /**
   * Biases result sizes to the last 5 minutes heavily. This histogram does not see all result
   * sizes. The executor only ever processes one posting at any given time and drops the rest.
   * {@link #resultPostingMeter} records the actual rate (i.e. sees all posting calls).
   */
  private final Histogram resultPostingSizes;
  private final Meter resultPostingMeter;
  /**
   * A single threaded bounded work queue to update result posting sizes.
   */
  private final ExecutorService resultPostingSizerExecutorService;
  @SuppressWarnings("rawtypes")
  private final TaskConverter<DataSubmissionTask> taskConverter;

  /**
   * Only size postings once every 5 seconds.
   */
  @SuppressWarnings("UnstableApiUsage")
  private final RateLimiter resultSizingRateLimier = RateLimiter.create(0.2);

  /**
   * @param handle metric pipeline handle (usually port number).
   */
  public TaskSizeEstimator(String handle) {
    this.resultPostingSizes = REGISTRY.newHistogram(new TaggedMetricName("post-result",
        "result-size", "port", handle), true);
    this.resultPostingMeter = REGISTRY.newMeter(new TaggedMetricName("post-result",
        "results", "port", handle), "results", TimeUnit.MINUTES);
    this.resultPostingSizerExecutorService = new ThreadPoolExecutor(1, 1,
        60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1),
        new NamedThreadFactory("result-posting-sizer-" + handle));
    // for now, we can just use a generic task converter with default lz4 compression method
    this.taskConverter = new RetryTaskConverter<>(handle, TaskConverter.CompressionType.LZ4);
    Metrics.newGauge(new TaggedMetricName("buffer", "fill-rate", "port", handle),
        new Gauge<Long>() {
          @Override
          public Long value() {
            return getBytesPerMinute();
          }
        });
  }

  /**
   * Submit a candidate task to be sized. The task may or may not be accepted, depending on
   * the rate limiter
   * @param task task to be sized.
   */
  public <T extends DataSubmissionTask<T>> void scheduleTaskForSizing(T task) {
    resultPostingMeter.mark();
    try {
      //noinspection UnstableApiUsage
      if (resultSizingRateLimier.tryAcquire()) {
        resultPostingSizerExecutorService.submit(getPostingSizerTask(task));
      }
    } catch (Exception ex) {
      // ignored.
    }
  }

  /**
   * Calculates the bytes per minute buffer usage rate. Needs at
   *
   * @return bytes per minute for requests submissions. Null if no data is available yet (needs
   *         at least
   */
  @Nullable
  public Long getBytesPerMinute() {
    if (resultPostingSizes.count() < 50) return null;
    if (resultPostingMeter.fifteenMinuteRate() == 0 || resultPostingSizes.mean() == 0) return null;
    return (long) (resultPostingSizes.mean() * resultPostingMeter.fifteenMinuteRate());
  }

  public void shutdown() {
    resultPostingSizerExecutorService.shutdown();
  }

  private <T extends DataSubmissionTask<T>> Runnable getPostingSizerTask(final T task) {
    return () -> {
      try {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        taskConverter.serializeToStream(task, outputStream);
        resultPostingSizes.update(outputStream.size());
      } catch (Throwable t) {
        // ignored. this is a stats task.
      }
    };
  }
}
