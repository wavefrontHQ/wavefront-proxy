package com.wavefront.agent.handlers;

import com.wavefront.agent.histogram.Granularity;
import com.wavefront.agent.histogram.HistogramKey;
import com.wavefront.agent.histogram.HistogramUtils;
import com.wavefront.agent.histogram.accumulator.Accumulator;
import com.wavefront.api.agent.ValidationConfiguration;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.wavefront.agent.histogram.HistogramUtils.granularityToString;
import static com.wavefront.common.Utils.lazySupplier;
import static com.wavefront.data.Validation.validatePoint;

/**
 * A ReportPointHandler that ships parsed points to a histogram accumulator instead of
 * forwarding them to SenderTask.
 *
 * @author vasily@wavefront.com
 */
public class HistogramAccumulationHandlerImpl extends ReportPointHandlerImpl {
  private final Accumulator digests;
  private final Granularity granularity;
  // Metrics
  private final Supplier<Counter> pointCounter;
  private final Supplier<Counter> pointRejectedCounter;
  private final Supplier<Counter> histogramCounter;
  private final Supplier<Counter> histogramRejectedCounter;
  private final Supplier<com.yammer.metrics.core.Histogram> histogramBinCount;
  private final Supplier<com.yammer.metrics.core.Histogram> histogramSampleCount;

  /**
   * Creates a new instance
   *
   * @param handlerKey           pipeline handler key
   * @param digests              accumulator for storing digests
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written
   *                             into the main log file.
   * @param granularity          granularity level
   * @param validationConfig     Supplier for the ValidationConfiguration
   * @param isHistogramInput     Whether expected input data for this handler is histograms.
   * @param receivedRateSink     Where to report received rate.
   */
  public HistogramAccumulationHandlerImpl(final HandlerKey handlerKey,
                                          final Accumulator digests,
                                          final int blockedItemsPerBatch,
                                          @Nullable Granularity granularity,
                                          @Nonnull final ValidationConfiguration validationConfig,
                                          boolean isHistogramInput,
                                          @Nullable final Consumer<Long> receivedRateSink,
                                          @Nullable final Logger blockedItemLogger,
                                          @Nullable final Logger validItemsLogger) {
    super(handlerKey, blockedItemsPerBatch, null, validationConfig, !isHistogramInput,
        receivedRateSink, blockedItemLogger, validItemsLogger, null);
    this.digests = digests;
    this.granularity = granularity;
    String metricNamespace = "histogram.accumulator." + granularityToString(granularity);
    pointCounter = lazySupplier(() ->
        Metrics.newCounter(new MetricName(metricNamespace, "", "sample_added")));
    pointRejectedCounter = lazySupplier(() ->
        Metrics.newCounter(new MetricName(metricNamespace, "", "sample_rejected")));
    histogramCounter = lazySupplier(() ->
        Metrics.newCounter(new MetricName(metricNamespace, "", "histogram_added")));
    histogramRejectedCounter = lazySupplier(() ->
        Metrics.newCounter(new MetricName(metricNamespace, "", "histogram_rejected")));
    histogramBinCount = lazySupplier(() ->
        Metrics.newHistogram(new MetricName(metricNamespace, "", "histogram_bins")));
    histogramSampleCount = lazySupplier(() ->
        Metrics.newHistogram(new MetricName(metricNamespace, "", "histogram_samples")));
  }

  @Override
  protected void reportInternal(ReportPoint point) {
    validatePoint(point, validationConfig);

    if (point.getValue() instanceof Double) {
      if (granularity == null) {
        pointRejectedCounter.get().inc();
        reject(point, "Wavefront data format is not supported on distribution ports!");
        return;
      }
      // Get key
      HistogramKey histogramKey = HistogramUtils.makeKey(point, granularity);
      double value = (Double) point.getValue();
      pointCounter.get().inc();

      // atomic update
      digests.put(histogramKey, value);
    } else if (point.getValue() instanceof Histogram) {
      Histogram value = (Histogram) point.getValue();
      Granularity pointGranularity = Granularity.fromMillis(value.getDuration());
      if (granularity != null && pointGranularity.getInMillis() > granularity.getInMillis()) {
        reject(point, "Attempting to send coarser granularity (" +
            granularityToString(pointGranularity) + ") distribution to a finer granularity (" +
            granularityToString(granularity) + ") port");
        histogramRejectedCounter.get().inc();
        return;
      }

      histogramBinCount.get().update(value.getCounts().size());
      histogramSampleCount.get().update(value.getCounts().stream().mapToLong(x -> x).sum());

      // Key
      HistogramKey histogramKey = HistogramUtils.makeKey(point,
          granularity == null ? pointGranularity : granularity);
      histogramCounter.get().inc();

      // atomic update
      digests.put(histogramKey, value);
    }

    if (validItemsLogger != null && validItemsLogger.isLoggable(Level.FINEST)) {
      validItemsLogger.info(serializer.apply(point));
    }
  }
}
