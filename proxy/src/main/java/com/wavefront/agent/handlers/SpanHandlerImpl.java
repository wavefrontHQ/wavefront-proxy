package com.wavefront.agent.handlers;

import com.wavefront.agent.SharedMetricsRegistry;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.SpanSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import wavefront.report.Span;

/**
 * Handler that processes incoming Span objects, validates them and hands them over to one of
 * the {@link SenderTask} threads.
 *
 * @author vasily@wavefront.com
 */
public class SpanHandlerImpl extends AbstractReportableEntityHandler<Span> {

  private static final Logger logger = Logger.getLogger(AbstractReportableEntityHandler.class.getCanonicalName());
  private static final Logger validTracesLogger = Logger.getLogger("RawValidTraces");
  private static final Random RANDOM = new Random();
  private static SharedMetricsRegistry metricsRegistry = SharedMetricsRegistry.getInstance();

  private final Counter attemptedCounter;
  private final Counter queuedCounter;
  private final Counter deliveredCounter;

  private boolean logData = false;
  private final double logSampleRate;
  private volatile long logStateUpdatedMillis = 0L;

  /**
   * Create new instance.
   *
   * @param handle               handle / port number.
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into the main log file.
   * @param sendDataTasks        sender tasks.
   */
  SpanHandlerImpl(final String handle,
                  final int blockedItemsPerBatch,
                  final Collection<SenderTask> sendDataTasks) {
    super(ReportableEntityType.TRACE, handle, blockedItemsPerBatch, new SpanSerializer(), sendDataTasks);

    String logTracesSampleRateProperty = System.getProperty("wavefront.proxy.logtraces.sample-rate");
    this.logSampleRate = NumberUtils.isNumber(logTracesSampleRateProperty) ?
        Double.parseDouble(logTracesSampleRateProperty) : 1.0d;

    this.attemptedCounter = Metrics.newCounter(new MetricName("traces." + handle, "", "sent"));
    this.queuedCounter = Metrics.newCounter(new MetricName("traces." + handle, "", "queued"));
    this.deliveredCounter = Metrics.newCounter(new MetricName("traces." + handle, "", "delivered"));

    this.statisticOutputExecutor.scheduleAtFixedRate(this::printStats, 10, 10, TimeUnit.SECONDS);
    this.statisticOutputExecutor.scheduleAtFixedRate(this::printTotal, 1, 1, TimeUnit.MINUTES);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void reportInternal(Span span) {
    //validatePoint(span, handle, Validation.Level.NUMERIC_ONLY);

    String strSpan = serializer.apply(span);

    refreshValidDataLoggerState();

    if (logData && (logSampleRate >= 1.0d || (logSampleRate > 0.0d && RANDOM.nextDouble() < logSampleRate))) {
      // we log valid trace data only if RawValidTraces log level is set to "ALL". This is done to prevent
      // introducing overhead and accidentally logging raw data to the main log. Honor sample rate limit, if set.
      validTracesLogger.info(strSpan);
    }
    /// TODO: REMOVE
    System.out.println("!!!! " + strSpan);
    getTask().add(strSpan);
    receivedMeter.mark();
    receivedCounter.inc();
  }

  private void refreshValidDataLoggerState() {
    if (logStateUpdatedMillis + TimeUnit.SECONDS.toMillis(1) < System.currentTimeMillis()) {
      // refresh validTracesLogger level once a second
      if (logData != validTracesLogger.isLoggable(Level.FINEST)) {
        logData = !logData;
        logger.info("Valid traces logging is now " + (logData ?
            "enabled with " + (logSampleRate * 100) + "% sampling":
            "disabled"));
      }
      logStateUpdatedMillis = System.currentTimeMillis();
    }
  }

  private void printStats() {
    logger.info("[" + this.handle + "] Traces received rate: " + Math.round(receivedMeter.oneMinuteRate()) +
        " tps (1 min), " + Math.round(receivedMeter.fiveMinuteRate()) + " tps (5 min), " +
        this.receivedBurstRateCurrent + " tps (current).");
  }

  private void printTotal() {
    logger.info("[" + this.handle + "] Total traces processed since start: " + this.attemptedCounter.count() +
        "; blocked: " + this.blockedCounter.count()); // + "; sent: " + this.deliveredCounter.count());

  }
}
