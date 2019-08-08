package com.wavefront.agent.handlers;

import com.wavefront.agent.histogram.Utils;
import com.wavefront.agent.histogram.accumulator.Accumulator;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.data.Validation;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.math.NumberUtils;

import java.util.Random;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

import static com.wavefront.agent.Utils.lazySupplier;
import static com.wavefront.agent.histogram.Utils.Granularity.fromMillis;
import static com.wavefront.agent.histogram.Utils.Granularity.granularityToString;
import static com.wavefront.data.Validation.validatePoint;

/**
 * A ReportPointHandler that ships parsed points to a histogram accumulator instead of
 * forwarding them to SenderTask.
 *
 * @author vasily@wavefront.com
 */
public class HistogramAccumulationHandlerImpl extends ReportPointHandlerImpl {
  private static final Logger logger = Logger.getLogger(
      AbstractReportableEntityHandler.class.getCanonicalName());
  private static final Logger validPointsLogger = Logger.getLogger("RawValidPoints");
  private static final Random RANDOM = new Random();

  private final Accumulator digests;
  private final long ttlMillis;
  private final Utils.Granularity granularity;
  private final short compression;
  // Metrics
  private final Supplier<Counter> pointCounter;
  private final Supplier<Counter> pointRejectedCounter;
  private final Supplier<Counter> histogramCounter;
  private final Supplier<Counter> histogramRejectedCounter;
  private final Supplier<com.yammer.metrics.core.Histogram> histogramBinCount;
  private final Supplier<com.yammer.metrics.core.Histogram> histogramSampleCount;

  private final double logSampleRate;
  /**
   * Value of system property wavefront.proxy.logpoints (for backwards compatibility)
   */
  private final boolean logPointsFlag;

  /**
   * Creates a new instance
   *
   * @param handle               handle/port number
   * @param digests              accumulator for storing digests
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written
   *                             into the main log file.
   * @param ttlMillis            default time-to-dispatch in milliseconds for new bins
   * @param granularity          granularity level
   * @param compression          default compression level for new bins
   * @param validationConfig     Supplier for the ValidationConfiguration
   * @param isHistogramInput     Whether expected input data for this handler is histograms.
   */
  public HistogramAccumulationHandlerImpl(
      final String handle, final Accumulator digests, final int blockedItemsPerBatch,
      long ttlMillis, @Nullable Utils.Granularity granularity, short compression,
      @Nullable final Supplier<ValidationConfiguration> validationConfig,
      boolean isHistogramInput) {
    super(handle, blockedItemsPerBatch, null, validationConfig, isHistogramInput, false);
    this.digests = digests;
    this.ttlMillis = ttlMillis;
    this.granularity = granularity;
    this.compression = compression;
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
    String logPointsProperty = System.getProperty("wavefront.proxy.logpoints");
    this.logPointsFlag = logPointsProperty != null && logPointsProperty.equalsIgnoreCase("true");
    String logPointsSampleRateProperty =
        System.getProperty("wavefront.proxy.logpoints.sample-rate");
    this.logSampleRate = NumberUtils.isNumber(logPointsSampleRateProperty) ?
        Double.parseDouble(logPointsSampleRateProperty) : 1.0d;
  }

  @Override
  protected void reportInternal(ReportPoint point) {
    if (validationConfig.get() == null) {
      validatePoint(point, handle, Validation.Level.NUMERIC_ONLY);
    } else {
      validatePoint(point, validationConfig.get());
    }

    if (point.getValue() instanceof Double) {
      if (granularity == null) {
        pointRejectedCounter.get().inc();
        reject(point, "Wavefront data format is not supported on distribution ports!");
        return;
      }
      // Get key
      Utils.HistogramKey histogramKey = Utils.makeKey(point, granularity);
      double value = (Double) point.getValue();
      pointCounter.get().inc();

      // atomic update
      digests.put(histogramKey, value, compression, ttlMillis);
    } else if (point.getValue() instanceof Histogram) {
      Histogram value = (Histogram) point.getValue();
      Utils.Granularity pointGranularity = fromMillis(value.getDuration());
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
      Utils.HistogramKey histogramKey = Utils.makeKey(point,
          granularity == null ? pointGranularity : granularity);
      histogramCounter.get().inc();

      // atomic update
      digests.put(histogramKey, value, compression, ttlMillis);
    }

    refreshValidPointsLoggerState();
    if ((logData || logPointsFlag) &&
        (logSampleRate >= 1.0d || (logSampleRate > 0.0d && RANDOM.nextDouble() < logSampleRate))) {
      // we log valid points only if system property wavefront.proxy.logpoints is true or
      // RawValidPoints log level is set to "ALL". this is done to prevent introducing overhead and
      // accidentally logging points to the main log, Additionally, honor sample rate limit, if set.
      validPointsLogger.info(serializer.apply(point));
    }
  }
}
