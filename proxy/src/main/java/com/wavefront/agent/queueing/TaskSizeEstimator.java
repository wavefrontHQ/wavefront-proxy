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
import net.jpountz.lz4.LZ4BlockOutputStream;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Calculates approximate task sizes (to estimate
 *
 * @author vasily@wavefront.com.
 */
public class TaskSizeEstimator {
  private static final MetricsRegistry metricsRegistry = SharedMetricsRegistry.getInstance();
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
  private final ExecutorService resultPostingSizerExecutorService = new ThreadPoolExecutor(1, 1,
      60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1),
      new NamedThreadFactory("result-posting-sizer"));

  /**
   * Only size postings once every 5 seconds.
   */
  private final RateLimiter resultSizingRateLimier = RateLimiter.create(0.2);

  public TaskSizeEstimator(String handle) {
    this.resultPostingSizes = metricsRegistry.newHistogram(new TaggedMetricName("post-result",
        "result-size", "port", handle), true);
    this.resultPostingMeter = metricsRegistry.newMeter(new TaggedMetricName("post-result",
        "results", "port", handle), "results", TimeUnit.MINUTES);
    Metrics.newGauge(new TaggedMetricName("buffer", "fill-rate", "port", handle),
        new Gauge<Long>() {
          @Override
          public Long value() {
            return getBytesPerMinute();
          }
        });
  }

  public void scheduleTaskForSizing(DataSubmissionTask task) {
    try {
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

  private Runnable getPostingSizerTask(final DataSubmissionTask task) {
    return () -> {
      try {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        LZ4BlockOutputStream lz4OutputStream = new LZ4BlockOutputStream(outputStream);
        ObjectOutputStream oos = new ObjectOutputStream(lz4OutputStream);
        oos.writeObject(task);
        oos.close();
        lz4OutputStream.close();
        resultPostingSizes.update(outputStream.size());
      } catch (Throwable t) {
        // ignored. this is a stats task.
      }
    };
  }
}
