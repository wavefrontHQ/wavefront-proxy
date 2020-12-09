package com.wavefront.agent.handlers;

import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.data.AnnotationUtils;
import com.wavefront.ingester.SpanSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.Span;
import wavefront.report.SpanLogs;

import static com.wavefront.agent.sampler.SpanSampler.SPAN_SAMPLING_POLICY_TAG;
import static com.wavefront.data.Validation.validateSpan;

/**
 * Handler that processes incoming Span objects, validates them and hands them over to one of
 * the {@link SenderTask} threads.
 *
 * @author vasily@wavefront.com
 */
public class SpanHandlerImpl extends AbstractReportableEntityHandler<Span, String> {

  private final ValidationConfiguration validationConfig;
  private final Logger validItemsLogger;
  private final Supplier<Integer> dropSpansDelayedMinutes;
  private final com.yammer.metrics.core.Histogram receivedTagCount;
  private final com.yammer.metrics.core.Counter policySampledSpanCounter;
  private final Supplier<ReportableEntityHandler<SpanLogs, String>> spanLogsHandler;


  /**
   * @param handlerKey              pipeline hanler key.
   * @param blockedItemsPerBatch    controls sample rate of how many blocked points are written into
   *                                the main log file.
   * @param sendDataTasks           sender tasks.
   * @param validationConfig        parameters for data validation.
   * @param receivedRateSink        where to report received rate.
   * @param blockedItemLogger       logger for blocked items.
   * @param validItemsLogger        logger for valid items.
   * @param dropSpansDelayedMinutes latency threshold for dropping delayed spans.
   * @param spanLogsHandler         spanLogs handler.
   */
  SpanHandlerImpl(final HandlerKey handlerKey,
                  final int blockedItemsPerBatch,
                  final Collection<SenderTask<String>> sendDataTasks,
                  @Nonnull final ValidationConfiguration validationConfig,
                  @Nullable final Consumer<Long> receivedRateSink,
                  @Nullable final Logger blockedItemLogger,
                  @Nullable final Logger validItemsLogger,
                  @Nonnull final Supplier<Integer> dropSpansDelayedMinutes,
                  @Nonnull final Supplier<ReportableEntityHandler<SpanLogs, String>> spanLogsHandler) {
    super(handlerKey, blockedItemsPerBatch, new SpanSerializer(), sendDataTasks, true,
        receivedRateSink, blockedItemLogger);
    this.validationConfig = validationConfig;
    this.validItemsLogger = validItemsLogger;
    this.dropSpansDelayedMinutes = dropSpansDelayedMinutes;
    this.receivedTagCount = Metrics.newHistogram(new MetricName(handlerKey.toString() +
        ".received", "", "tagCount"), false);
    this.spanLogsHandler = spanLogsHandler;
    this.policySampledSpanCounter = Metrics.newCounter(new MetricName(handlerKey.toString(), "",
        "sampler.policy.saved"));
  }

  @Override
  protected void reportInternal(Span span) {
    receivedTagCount.update(span.getAnnotations().size());
    Integer maxSpanDelay = dropSpansDelayedMinutes.get();
    if (maxSpanDelay != null && span.getStartMillis() + span.getDuration() <
        Clock.now() - TimeUnit.MINUTES.toMillis(maxSpanDelay)) {
      this.reject(span, "span is older than acceptable delay of " + maxSpanDelay + " minutes");
      return;
    }
    validateSpan(span, validationConfig, spanLogsHandler.get()::report);
    if (span.getAnnotations() != null && AnnotationUtils.getValue(span.getAnnotations(),
        SPAN_SAMPLING_POLICY_TAG) != null) {
      this.policySampledSpanCounter.inc();
    }
    final String strSpan = serializer.apply(span);
    getTask().add(strSpan);
    getReceivedCounter().inc();
    if (validItemsLogger != null) validItemsLogger.info(strSpan);
  }
}
