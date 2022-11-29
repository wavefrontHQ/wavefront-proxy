package com.wavefront.agent.core.handlers;

import static com.wavefront.agent.PushAgent.isMulticastingActive;
import static com.wavefront.data.Validation.validatePoint;

import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.common.Utils;
import com.wavefront.data.DeltaCounterValueException;
import com.wavefront.ingester.ReportPointSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

/**
 * Handler that processes incoming ReportPoint objects, validates them and hands them over to one of
 * the SenderTask threads.
 */
class ReportPointHandlerImpl extends AbstractReportableEntityHandler<ReportPoint, String> {
  private static final Logger logger =
      Logger.getLogger(ReportPointHandlerImpl.class.getCanonicalName());

  final ValidationConfiguration validationConfig;
  final Function<Histogram, Histogram> recompressor;
  final com.yammer.metrics.core.Histogram receivedPointLag;
  final com.yammer.metrics.core.Histogram receivedTagCount;
  final Supplier<Counter> discardedCounterSupplier;

  /**
   * Creates a new instance that handles either histograms or points.
   *
   * @param handlerKey handler key for the metrics pipeline.
   * @param validationConfig validation configuration.
   * @param blockedItemLogger logger for blocked items (optional).
   * @param recompressor histogram recompressor (optional)
   */
  ReportPointHandlerImpl(
      final String handler,
      final QueueInfo handlerKey,
      @Nonnull final ValidationConfiguration validationConfig,
      @Nullable final Logger blockedItemLogger,
      @Nullable final Function<Histogram, Histogram> recompressor) {
    super(handler, handlerKey, new ReportPointSerializer(), blockedItemLogger);
    this.validationConfig = validationConfig;
    this.recompressor = recompressor;
    MetricsRegistry registry = Metrics.defaultRegistry();
    this.receivedPointLag =
        registry.newHistogram(new MetricName(handlerKey.getName() + ".received", "", "lag"), false);
    this.receivedTagCount =
        registry.newHistogram(
            new MetricName(handlerKey.getName() + ".received", "", "tagCount"), false);
    this.discardedCounterSupplier =
        Utils.lazySupplier(
            () -> Metrics.newCounter(new MetricName(handlerKey.toString(), "", "discarded")));
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
    final String strPoint = serializer.apply(point);

    incrementReceivedCounters(strPoint.length());
    BuffersManager.sendMsg(queue, strPoint);

    if (isMulticastingActive
        && point.getAnnotations() != null
        && point.getAnnotations().containsKey(MULTICASTING_TENANT_TAG_KEY)) {
      String[] multicastingTenantNames =
          point.getAnnotations().get(MULTICASTING_TENANT_TAG_KEY).trim().split(",");
      point.getAnnotations().remove(MULTICASTING_TENANT_TAG_KEY);
      for (String tenant : multicastingTenantNames) {
        QueueInfo tenantQueue = queue.getTenantQueue(tenant);
        if (tenantQueue != null) {
          BuffersManager.sendMsg(tenantQueue, strPoint);
        } else {
          logger.info("Tenant '" + tenant + "' invalid");
        }
      }
    }
  }
}
