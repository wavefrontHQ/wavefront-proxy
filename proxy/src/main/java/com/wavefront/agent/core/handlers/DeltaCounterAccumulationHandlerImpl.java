package com.wavefront.agent.core.handlers;

import static com.wavefront.agent.PushAgent.isMulticastingActive;
import static com.wavefront.data.Validation.validatePoint;
import static com.wavefront.sdk.common.Utils.metricToLineData;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.common.HostMetricTagsPair;
import com.wavefront.common.Utils;
import com.wavefront.data.DeltaCounterValueException;
import com.wavefront.ingester.ReportPointSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import java.util.Objects;
import java.util.Timer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import wavefront.report.ReportPoint;

/**
 * Handler that processes incoming DeltaCounter objects, aggregates them and hands it over to one of
 * the {@link SenderTask} threads according to deltaCountersAggregationIntervalSeconds or before
 * cache expires.
 */
public class DeltaCounterAccumulationHandlerImpl
    extends AbstractReportableEntityHandler<ReportPoint, String> {

  private static final Logger log =
      LogManager.getLogger(DeltaCounterAccumulationHandlerImpl.class.getCanonicalName());

  final Histogram receivedPointLag;
  private final ValidationConfiguration validationConfig;
  private final BurstRateTrackingCounter reportedStats;
  private final Supplier<Counter> discardedCounterSupplier;
  private final Cache<HostMetricTagsPair, AtomicDouble> aggregatedDeltas;
  private final ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
  private final Timer receivedRateTimer;

  /**
   * @param handlerKey metrics pipeline key.
   * @param validationConfig validation configuration.
   * @param aggregationIntervalSeconds aggregation interval for delta counters.
   * @param blockedItemLogger logger for blocked items.
   */
  public DeltaCounterAccumulationHandlerImpl(
      final String handler,
      final QueueInfo handlerKey,
      @Nonnull final ValidationConfiguration validationConfig,
      long aggregationIntervalSeconds,
      @Nullable final Logger blockedItemLogger) {
    super(handler, handlerKey, new ReportPointSerializer(), blockedItemLogger);
    this.validationConfig = validationConfig;

    this.aggregatedDeltas =
        Caffeine.newBuilder()
            .expireAfterAccess(5 * aggregationIntervalSeconds, TimeUnit.SECONDS)
            .removalListener(
                (RemovalListener<HostMetricTagsPair, AtomicDouble>)
                    (metric, value, reason) -> this.reportAggregatedDeltaValue(metric, value))
            .build();

    this.receivedPointLag =
        Metrics.newHistogram(
            new MetricName("points." + handlerKey.getName() + ".received", "", "lag"), false);

    reporter.scheduleWithFixedDelay(
        this::flushDeltaCounters,
        aggregationIntervalSeconds,
        aggregationIntervalSeconds,
        TimeUnit.SECONDS);

    String metricPrefix = handlerKey.toString();
    this.reportedStats =
        new BurstRateTrackingCounter(
            new MetricName(metricPrefix, "", "sent"), Metrics.defaultRegistry(), 1000);
    this.discardedCounterSupplier =
        Utils.lazySupplier(() -> Metrics.newCounter(new MetricName(metricPrefix, "", "discarded")));
    Metrics.newGauge(
        new MetricName(metricPrefix, "", "accumulator.size"),
        new Gauge<Long>() {
          @Override
          public Long value() {
            return aggregatedDeltas.estimatedSize();
          }
        });
    this.receivedRateTimer = new Timer("delta-counter-timer-" + handlerKey.getName());
    // TODO: review
    //      this.receivedRateTimer.scheduleAtFixedRate(
    //          new TimerTask() {
    //            @Override
    //            public void run() {
    //              for (String tenantName : senderTaskMap.keySet()) {
    //                receivedRateSink.accept(tenantName, receivedStats.getCurrentRate());
    //              }
    //            }
    //          },
    //          1000,
    //          1000);
  }

  @VisibleForTesting
  public void flushDeltaCounters() {
    this.aggregatedDeltas.asMap().forEach(this::reportAggregatedDeltaValue);
  }

  private void reportAggregatedDeltaValue(
      @Nullable HostMetricTagsPair hostMetricTagsPair, @Nullable AtomicDouble value) {
    if (value == null || hostMetricTagsPair == null) {
      return;
    }
    this.reportedStats.inc();
    double reportedValue = value.getAndSet(0);
    if (reportedValue == 0) return;
    String strPoint =
        metricToLineData(
            hostMetricTagsPair.metric,
            reportedValue,
            Clock.now(),
            hostMetricTagsPair.getHost(),
            hostMetricTagsPair.getTags(),
            "wavefront-proxy");

    getReceivedCounter().inc();
    BuffersManager.sendMsg(queue, strPoint);

    if (isMulticastingActive
        && hostMetricTagsPair.getTags() != null
        && hostMetricTagsPair.getTags().containsKey(MULTICASTING_TENANT_TAG_KEY)) {
      String[] multicastingTenantNames =
          hostMetricTagsPair.getTags().get(MULTICASTING_TENANT_TAG_KEY).trim().split(",");
      hostMetricTagsPair.getTags().remove(MULTICASTING_TENANT_TAG_KEY);
      for (String tenant : multicastingTenantNames) {
        QueueInfo tenantQueue = queue.getTenantQueue(tenant);
        if (tenantQueue != null) {
          BuffersManager.sendMsg(tenantQueue, strPoint);
        } else {
          log.info("Tenant '" + tenant + "' invalid");
        }
      }
    }
  }

  @Override
  void reportInternal(ReportPoint point) {
    if (DeltaCounter.isDelta(point.getMetric())) {
      try {
        validatePoint(point, validationConfig);
      } catch (DeltaCounterValueException e) {
        discardedCounterSupplier.get().inc();
        return;
      }
      getReceivedCounter().inc();
      double deltaValue = (double) point.getValue();
      receivedPointLag.update(Clock.now() - point.getTimestamp());
      HostMetricTagsPair hostMetricTagsPair =
          new HostMetricTagsPair(point.getHost(), point.getMetric(), point.getAnnotations());
      Objects.requireNonNull(aggregatedDeltas.get(hostMetricTagsPair, key -> new AtomicDouble(0)))
          .getAndAdd(deltaValue);
    } else {
      reject(point, "Port is not configured to accept non-delta counter data!");
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
    reporter.shutdown();
    if (receivedRateTimer != null) {
      receivedRateTimer.cancel();
    }
  }
}
