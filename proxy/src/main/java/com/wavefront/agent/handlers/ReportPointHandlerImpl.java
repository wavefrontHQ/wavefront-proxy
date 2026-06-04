package com.wavefront.agent.handlers;

import static com.wavefront.data.Validation.validatePoint;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.sampler.MetricBloomFilterSampler;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.api.agent.preprocessor.ReportPointSampleInclude;
import com.wavefront.common.Clock;
import com.wavefront.common.Utils;
import com.wavefront.data.DeltaCounterValueException;
import com.wavefront.ingester.ReportPointSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.BurstRateTrackingCounter;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

/**
 * Handler that processes incoming ReportPoint objects, validates them and hands them over to one of
 * the {@link SenderTask} threads.
 *
 * @author vasily@wavefront.com
 */
class ReportPointHandlerImpl extends AbstractReportableEntityHandler<ReportPoint, String> {
  private static final Logger logger =
          Logger.getLogger(ReportPointHandlerImpl.class.getCanonicalName());

  final Logger validItemsLogger;
  final ValidationConfiguration validationConfig;
  final Function<Histogram, Histogram> recompressor;
  final com.yammer.metrics.core.Histogram receivedPointLag;
  final com.yammer.metrics.core.Histogram receivedTagCount;
  final Supplier<Counter> discardedCounterSupplier;
  final Supplier<Counter> sampledCounterSupplier;
  final BurstRateTrackingCounter sampledOutStats;
  final MetricBloomFilterSampler metricBloomFilterSampler;

  /**
   * Creates a new instance that handles either histograms or points.
   *
   * @param handlerKey handler key for the metrics pipeline.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into
   *     the main log file.
   * @param senderTaskMap map of tenant name and tasks actually handling data transfer to the
   *     Wavefront endpoint corresponding to the tenant name
   * @param validationConfig validation configuration.
   * @param setupMetrics Whether we should report counter metrics.
   * @param receivedRateSink Where to report received rate.
   * @param blockedItemLogger logger for blocked items (optional).
   * @param validItemsLogger sampling logger for valid items (optional).
   * @param recompressor histogram recompressor (optional)
   * @param metricBloomFilterSampler metric sampler (optional, points only).
   */
  ReportPointHandlerImpl(
      final HandlerKey handlerKey,
      final int blockedItemsPerBatch,
      @Nullable final Map<String, Collection<SenderTask<String>>> senderTaskMap,
      @Nonnull final ValidationConfiguration validationConfig,
      final boolean setupMetrics,
      @Nullable final BiConsumer<String, Long> receivedRateSink,
      @Nullable final Logger blockedItemLogger,
      @Nullable final Logger validItemsLogger,
      @Nullable final Function<Histogram, Histogram> recompressor,
      @Nullable final MetricBloomFilterSampler metricBloomFilterSampler) {
    super(
        handlerKey,
        blockedItemsPerBatch,
        new ReportPointSerializer(),
        senderTaskMap,
        setupMetrics,
        receivedRateSink,
        blockedItemLogger);
    super.initializeCounters();
    this.validationConfig = validationConfig;
    this.validItemsLogger = validItemsLogger;
    this.recompressor = recompressor;
    this.metricBloomFilterSampler = metricBloomFilterSampler;
    MetricsRegistry registry = setupMetrics ? Metrics.defaultRegistry() : LOCAL_REGISTRY;
    this.receivedPointLag =
        registry.newHistogram(
            new MetricName(handlerKey.toString() + ".received", "", "lag"), false);
    this.receivedTagCount =
        registry.newHistogram(
            new MetricName(handlerKey.toString() + ".received", "", "tagCount"), false);
    this.discardedCounterSupplier =
        Utils.lazySupplier(
            () -> Metrics.newCounter(new MetricName(handlerKey.toString(), "", "discarded")));
    this.sampledCounterSupplier =
            Utils.lazySupplier(
                    () -> Metrics.newCounter(new MetricName(handlerKey.toString(), "", "sampled")));
    this.sampledOutStats =
            new BurstRateTrackingCounter(
                    new MetricName(handlerKey.toString(), "", "sampled-burst-rate"), registry, 1000 );
  }

  @Override
  void reportInternal(ReportPoint point) {
    receivedTagCount.update(point.getAnnotations().size());
    try {
      validatePoint(point, validationConfig);
    } catch (DeltaCounterValueException e) {
      discardedCounterSupplier.get().inc();
      return;
    }
    receivedPointLag.update(Clock.now() - point.getTimestamp());
    if (point.getValue() instanceof Histogram && recompressor != null) {
      Histogram histogram = (Histogram) point.getValue();
      point.setValue(recompressor.apply(histogram));
    }
    // Count as received once proxy accepts and validates point, even if sampled out later.
    getReceivedCounter().inc();
    if (metricBloomFilterSampler != null && !metricBloomFilterSampler.isForceDisableSampling() &&
            metricBloomFilterSampler.shouldSampleOut(point)) {
      discardedCounterSupplier.get().inc();
      sampledCounterSupplier.get().inc();
      sampledOutStats.inc();
      return;
    }
    // always try remove sample tag
    point.getAnnotations().remove(ReportPointSampleInclude.SAMPLING_TAG);

    final String strPoint = serializer.apply(point);
    getTask(APIContainer.CENTRAL_TENANT_NAME).add(strPoint);
    // check if data points contains the tag key indicating this point should be multicasted
    if (isMulticastingActive
        && point.getAnnotations() != null
        && point.getAnnotations().containsKey(MULTICASTING_TENANT_TAG_KEY)) {
      String[] multicastingTenantNames =
          point.getAnnotations().get(MULTICASTING_TENANT_TAG_KEY).trim().split(",");
      point.getAnnotations().remove(MULTICASTING_TENANT_TAG_KEY);
      for (String multicastingTenantName : multicastingTenantNames) {
        // if the tenant name indicated in point tag is not configured, just ignore
        if (getTask(multicastingTenantName) != null) {
          getTask(multicastingTenantName).add(serializer.apply(point));
        }
      }
    }
    if (validItemsLogger != null) validItemsLogger.info(strPoint);
  }

  @Override
  protected void printStats() {
    super.printStats();
    if (!reportReceivedStats) return;
    if (receivedStats.getFiveMinuteCount() == 0 && sampledOutStats.getFiveMinuteCount() == 0) {
      return;
    }
    logger.info(
            "["
                    + handlerKey.getHandle()
                    + "] "
                    + handlerKey.getEntityType().toCapitalizedString()
                    + " sampled out rate: "
                    + sampledOutStats.getOneMinutePrintableRate()
                    + " "
                    + rateUnit
                    + " (1 min), "
                    + sampledOutStats.getFiveMinutePrintableRate()
                    + " "
                    + rateUnit
                    + " (5 min), "
                    + sampledOutStats.getCurrentRate()
                    + " "
                    + rateUnit
                    + " (current).");
  }
}
