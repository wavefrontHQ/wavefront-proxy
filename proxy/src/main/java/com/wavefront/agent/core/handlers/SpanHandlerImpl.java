package com.wavefront.agent.core.handlers;

import static com.wavefront.agent.PushAgent.isMulticastingActive;
import static com.wavefront.agent.sampler.SpanSampler.SPAN_SAMPLING_POLICY_TAG;
import static com.wavefront.data.Validation.validateSpan;

import com.wavefront.agent.api.APIContainer;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.senders.SenderTask;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.data.AnnotationUtils;
import com.wavefront.ingester.SpanSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

/**
 * Handler that processes incoming Span objects, validates them and hands them over to one of the
 * {@link SenderTask} threads.
 *
 * @author vasily@wavefront.com
 */
public class SpanHandlerImpl extends AbstractReportableEntityHandler<Span, String> {
  private static final Logger log = Logger.getLogger(SpanHandlerImpl.class.getCanonicalName());

  private final ValidationConfiguration validationConfig;
  private final Logger validItemsLogger;
  private final Function<String, Integer> dropSpansDelayedMinutes;
  private final com.yammer.metrics.core.Histogram receivedTagCount;
  private final com.yammer.metrics.core.Counter policySampledSpanCounter;
  private final Supplier<ReportableEntityHandler<SpanLogs>> spanLogsHandler;

  /**
   * @param handlerKey pipeline hanler key.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into
   *     the main log file.
   * @param validationConfig parameters for data validation.
   * @param blockedItemLogger logger for blocked items.
   * @param validItemsLogger logger for valid items.
   * @param dropSpansDelayedMinutes latency threshold for dropping delayed spans.
   * @param spanLogsHandler spanLogs handler.
   */
  SpanHandlerImpl(
      final String handler,
      final QueueInfo handlerKey,
      final int blockedItemsPerBatch,
      @Nonnull final ValidationConfiguration validationConfig,
      @Nullable final Logger blockedItemLogger,
      @Nullable final Logger validItemsLogger,
      @Nonnull final Function<String, Integer> dropSpansDelayedMinutes,
      @Nonnull final Supplier<ReportableEntityHandler<SpanLogs>> spanLogsHandler) {
    super(handler, handlerKey, blockedItemsPerBatch, new SpanSerializer(), blockedItemLogger);
    this.validationConfig = validationConfig;
    this.validItemsLogger = validItemsLogger;
    this.dropSpansDelayedMinutes = dropSpansDelayedMinutes;
    this.receivedTagCount =
        Metrics.newHistogram(
            new MetricName(handlerKey.getName() + ".received", "", "tagCount"), false);
    this.spanLogsHandler = spanLogsHandler;
    this.policySampledSpanCounter =
        Metrics.newCounter(new MetricName(handlerKey.toString(), "", "sampler.policy.saved"));
  }

  // MONIT-26010: this is a temp helper function to remove MULTICASTING_TENANT_TAG
  // TODO: refactor this into AnnotationUtils or figure out a better removing implementation
  private static void removeSpanAnnotation(List<Annotation> annotations, String key) {
    Annotation toRemove = null;
    for (Annotation annotation : annotations) {
      if (annotation.getKey().equals(key)) {
        toRemove = annotation;
        // we should have only one matching
        break;
      }
    }
    annotations.remove(toRemove);
  }

  @Override
  protected void reportInternal(Span span) {
    receivedTagCount.update(span.getAnnotations().size());
    Integer maxSpanDelay = dropSpansDelayedMinutes.apply(APIContainer.CENTRAL_TENANT_NAME);
    if (maxSpanDelay != null
        && span.getStartMillis() + span.getDuration()
            < Clock.now() - TimeUnit.MINUTES.toMillis(maxSpanDelay)) {
      this.reject(span, "span is older than acceptable delay of " + maxSpanDelay + " minutes");
      return;
    }
    // Spans cannot exceed 24 hours future fill
    if (span.getStartMillis() > Clock.now() + TimeUnit.HOURS.toMillis(24)) {
      this.reject(span, "Span outside of reasonable timeframe");
      return;
    }
    // PUB-323 Allow "*" in span name by converting "*" to "-"
    if (span.getName().contains("*")) {
      span.setName(span.getName().replace('*', '-'));
    }
    validateSpan(span, validationConfig, spanLogsHandler.get()::report);
    if (span.getAnnotations() != null
        && AnnotationUtils.getValue(span.getAnnotations(), SPAN_SAMPLING_POLICY_TAG) != null) {
      this.policySampledSpanCounter.inc();
    }
    final String strSpan = serializer.apply(span);

    getReceivedCounter().inc();
    BuffersManager.sendMsg(queue, strSpan);

    if (isMulticastingActive
        && span.getAnnotations() != null
        && AnnotationUtils.getValue(span.getAnnotations(), MULTICASTING_TENANT_TAG_KEY) != null) {
      String[] multicastingTenantNames =
          AnnotationUtils.getValue(span.getAnnotations(), MULTICASTING_TENANT_TAG_KEY)
              .trim()
              .split(",");
      removeSpanAnnotation(span.getAnnotations(), MULTICASTING_TENANT_TAG_KEY);
      for (String tenant : multicastingTenantNames) {
        QueueInfo tenantQueue = queue.getTenantQueue(tenant);
        if (tenantQueue != null) {
          BuffersManager.sendMsg(tenantQueue, strSpan);
        } else {
          // TODO: rate
          log.fine("Tenant '" + tenant + "' invalid");
        }
      }
    }

    if (validItemsLogger != null) validItemsLogger.info(strSpan);
  }
}
