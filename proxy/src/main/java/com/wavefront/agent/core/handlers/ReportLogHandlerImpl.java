package com.wavefront.agent.core.handlers;

import static com.wavefront.data.Validation.validateLog;

import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.dto.Log;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.function.Function;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

/** This class will validate parsed logs and distribute them among SenderTask threads. */
public class ReportLogHandlerImpl extends AbstractReportableEntityHandler<ReportLog, Log> {
  private static final Function<ReportLog, String> LOG_SERIALIZER =
      value -> new Log(value).toString();
  final ValidationConfiguration validationConfig;
  final com.yammer.metrics.core.Histogram receivedLogLag;
  final com.yammer.metrics.core.Histogram receivedTagCount;
  final com.yammer.metrics.core.Histogram receivedTagLength;
  final com.yammer.metrics.core.Histogram receivedMessageLength;
  final com.yammer.metrics.core.Counter receivedByteCount;

  /**
   * @param handlerKey pipeline key.
   * @param validationConfig validation configuration.
   * @param blockedLogsLogger logger for blocked logs.
   */
  public ReportLogHandlerImpl(
      final String handler,
      final QueueInfo handlerKey,
      @Nonnull final ValidationConfiguration validationConfig,
      @Nullable final Logger blockedLogsLogger) {
    super(handler, handlerKey, LOG_SERIALIZER, blockedLogsLogger);
    this.validationConfig = validationConfig;
    MetricsRegistry registry = Metrics.defaultRegistry();
    this.receivedLogLag =
        registry.newHistogram(new MetricName(handlerKey.getName() + ".received", "", "lag"), false);
    this.receivedTagCount =
        registry.newHistogram(
            new MetricName(handlerKey.getName() + ".received", "", "tagCount"), false);
    this.receivedTagLength =
        registry.newHistogram(
            new MetricName(handlerKey.getName() + ".received", "", "tagLength"), false);
    this.receivedMessageLength =
        registry.newHistogram(
            new MetricName(handlerKey.getName() + ".received", "", "messageLength"), false);
    this.receivedByteCount =
        registry.newCounter(new MetricName(handlerKey + ".received", "", "bytes"));
  }

  @Override
  protected void reportInternal(ReportLog log) {
    receivedTagCount.update(log.getAnnotations().size());
    receivedMessageLength.update(log.getMessage().length());
    for (Annotation a : log.getAnnotations()) {
      receivedTagLength.update(a.getValue().length());
    }
    validateLog(log, validationConfig);
    receivedLogLag.update(Clock.now() - log.getTimestamp());
    Log logObj = new Log(log);
    receivedByteCount.inc(logObj.toString().getBytes().length);

    getReceivedCounter().inc();
    BuffersManager.sendMsg(queue, logObj.toString());
  }
}
