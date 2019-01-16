package com.wavefront.agent.handlers;

import com.wavefront.common.Clock;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.data.Validation;
import com.wavefront.ingester.ReportPointSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.math.NumberUtils;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import wavefront.report.ReportPoint;

import static com.wavefront.data.Validation.validatePoint;

/**
 * Handler that processes incoming ReportPoint objects, validates them and hands them over to one of
 * the {@link SenderTask} threads.
 *
 * @author vasily@wavefront.com
 */
class ReportPointHandlerImpl extends AbstractReportableEntityHandler<ReportPoint> {

  private static final Logger logger = Logger.getLogger(AbstractReportableEntityHandler.class.getCanonicalName());
  private static final Logger validPointsLogger = Logger.getLogger("RawValidPoints");
  private static final Random RANDOM = new Random();

  private final Counter attemptedCounter;
  private final Counter queuedCounter;
  private final Histogram receivedPointLag;

  private boolean logData = false;
  private final double logSampleRate;
  private volatile long logStateUpdatedMillis = 0L;

  /**
   * Value of system property wavefront.proxy.logpoints (for backwards compatibility)
   */
  private final boolean logPointsFlag;

  /**
   * Create new instance.
   *
   * @param handle               handle/port number
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into the main log file.
   * @param senderTasks          sender tasks
   */
  ReportPointHandlerImpl(final String handle,
                         final int blockedItemsPerBatch,
                         final Collection<SenderTask> senderTasks) {
    super(ReportableEntityType.POINT, handle, blockedItemsPerBatch, new ReportPointSerializer(), senderTasks);
    String logPointsProperty = System.getProperty("wavefront.proxy.logpoints");
    this.logPointsFlag = logPointsProperty != null && logPointsProperty.equalsIgnoreCase("true");
    String logPointsSampleRateProperty = System.getProperty("wavefront.proxy.logpoints.sample-rate");
    this.logSampleRate = NumberUtils.isNumber(logPointsSampleRateProperty) ?
        Double.parseDouble(logPointsSampleRateProperty) : 1.0d;

    this.receivedPointLag = Metrics.newHistogram(new MetricName("points." + handle + ".received", "", "lag"));
    this.attemptedCounter = Metrics.newCounter(new MetricName("points." + handle, "", "sent"));
    this.queuedCounter = Metrics.newCounter(new MetricName("points." + handle, "", "queued"));

    this.statisticOutputExecutor.scheduleAtFixedRate(this::printStats, 10, 10, TimeUnit.SECONDS);
    this.statisticOutputExecutor.scheduleAtFixedRate(this::printTotal, 1, 1, TimeUnit.MINUTES);
  }

  @Override
  @SuppressWarnings("unchecked")
  void reportInternal(ReportPoint point) {
    validatePoint(point, handle, Validation.Level.NUMERIC_ONLY);

    String strPoint = serializer.apply(point);

    refreshValidPointsLoggerState();

    if ((logData || logPointsFlag) &&
        (logSampleRate >= 1.0d || (logSampleRate > 0.0d && RANDOM.nextDouble() < logSampleRate))) {
      // we log valid points only if system property wavefront.proxy.logpoints is true or RawValidPoints log level is
      // set to "ALL". this is done to prevent introducing overhead and accidentally logging points to the main log
      // Additionally, honor sample rate limit, if set.
      validPointsLogger.info(strPoint);
    }
    getTask().add(strPoint);
    receivedCounter.inc();
    receivedPointLag.update(Clock.now() - point.getTimestamp());
  }

  private void refreshValidPointsLoggerState() {
    if (logStateUpdatedMillis + TimeUnit.SECONDS.toMillis(1) < System.currentTimeMillis()) {
      // refresh validPointsLogger level once a second
      if (logData != validPointsLogger.isLoggable(Level.FINEST)) {
        logData = !logData;
        logger.info("Valid points logging is now " + (logData ?
            "enabled with " + (logSampleRate * 100) + "% sampling":
            "disabled"));
      }
      logStateUpdatedMillis = System.currentTimeMillis();
    }
  }

  private void printStats() {
    logger.info("[" + this.handle + "] Points received rate: " + this.getReceivedOneMinuteRate() +
        " pps (1 min), " + getReceivedFiveMinuteRate() + " pps (5 min), " +
        this.receivedBurstRateCurrent + " pps (current).");
  }

  private void printTotal() {
    logger.info("[" + this.handle + "] Total points processed since start: " + this.attemptedCounter.count() +
        "; blocked: " + this.blockedCounter.count());
  }
}
