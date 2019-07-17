package com.wavefront.agent.handlers;

import com.wavefront.api.agent.ValidationConfiguration;
import com.wavefront.common.Clock;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.data.Validation;
import com.wavefront.ingester.ReportPointSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.math.NumberUtils;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

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

  private final Histogram receivedPointLag;

  private boolean logData = false;
  private final double logSampleRate;
  private volatile long logStateUpdatedMillis = 0L;

  /**
   * Value of system property wavefront.proxy.logpoints (for backwards compatibility)
   */
  private final boolean logPointsFlag;

  /**
   * Creates a new instance that handles either histograms or points.
   *
   * @param handle               handle/port number
   * @param blockedItemsPerBatch controls sample rate of how many blocked points are written into the main log file.
   * @param senderTasks          sender tasks
   * @param validationConfig     Supplier for the validation configuration.
   * @param isHistogramHandler   Whether this handler processes histograms (handles regular points if false).
   */
  ReportPointHandlerImpl(final String handle,
                         final int blockedItemsPerBatch,
                         final Collection<SenderTask> senderTasks,
                         @Nullable final Supplier<ValidationConfiguration> validationConfig,
                         final boolean isHistogramHandler) {
    super(isHistogramHandler ? ReportableEntityType.HISTOGRAM : ReportableEntityType.POINT, handle,
        blockedItemsPerBatch, new ReportPointSerializer(), senderTasks, validationConfig,
        isHistogramHandler ? "dps" : "pps");
    String logPointsProperty = System.getProperty("wavefront.proxy.logpoints");
    this.logPointsFlag = logPointsProperty != null && logPointsProperty.equalsIgnoreCase("true");
    String logPointsSampleRateProperty = System.getProperty("wavefront.proxy.logpoints.sample-rate");
    this.logSampleRate = NumberUtils.isNumber(logPointsSampleRateProperty) ?
        Double.parseDouble(logPointsSampleRateProperty) : 1.0d;

    this.receivedPointLag = Metrics.newHistogram(new MetricName("points." + handle + ".received", "", "lag"));
  }

  @Override
  @SuppressWarnings("unchecked")
  void reportInternal(ReportPoint point) {
    if (validationConfig.get() == null) {
      validatePoint(point, handle, Validation.Level.NUMERIC_ONLY);
    } else {
      validatePoint(point, validationConfig.get());
    }

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
        logger.info("Valid " + entityType.toString() + " logging is now " + (logData ?
            "enabled with " + (logSampleRate * 100) + "% sampling" :
            "disabled"));
      }
      logStateUpdatedMillis = System.currentTimeMillis();
    }
  }
}
