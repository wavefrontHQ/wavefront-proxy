package com.wavefront.common;

import com.google.common.base.Preconditions;
import com.wavefront.data.ReportableEntityType;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A sampling logger that can be enabled and disabled dynamically
 * by setting its level to {@code Level.FINEST}.
 *
 * @author vasily@wavefront.com
 */
public class SamplingLogger extends Logger {
  private static final Logger logger = Logger.getLogger(SamplingLogger.class.getCanonicalName());
  private static final Random RANDOM = new Random();

  private final Logger delegate;
  private final ReportableEntityType entityType;
  private final double samplingRate;
  private final boolean alwaysActive;
  private final AtomicBoolean loggingActive = new AtomicBoolean(false);

  /**
   * @param entityType   entity type (used in info messages only).
   * @param loggerName   logger name.
   * @param samplingRate sampling rate for logging [0..1].
   * @param alwaysActive whether this logger is always active regardless of currently set log level.
   */
  public SamplingLogger(ReportableEntityType entityType,
                        String loggerName,
                        double samplingRate,
                        boolean alwaysActive) {
    super(loggerName, null);
    Preconditions.checkArgument(samplingRate >= 0, "Sampling rate should be positive!");
    Preconditions.checkArgument(samplingRate <= 1, "Sampling rate should not be be > 1!");
    this.delegate = Logger.getLogger(loggerName);
    this.entityType = entityType;
    this.samplingRate = samplingRate;
    this.alwaysActive = alwaysActive;
    new Timer("Timer-sampling-logger-" + loggerName).scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        refreshLoggerState();
      }
    }, 1000, 1000);
  }

  @Override
  public void severe(String msg) {
    sampleAndLog(Level.SEVERE, msg);
  }

  @Override
  public void warning(String msg) {
    sampleAndLog(Level.WARNING, msg);
  }

  @Override
  public void info(String msg) {
    sampleAndLog(Level.INFO, msg);
  }

  @Override
  public void config(String msg) {
    sampleAndLog(Level.CONFIG, msg);
  }

  @Override
  public void fine(String msg) {
    sampleAndLog(Level.FINE, msg);
  }

  @Override
  public void finer(String msg) {
    sampleAndLog(Level.FINER, msg);
  }

  @Override
  public void finest(String msg) {
    sampleAndLog(Level.FINEST, msg);
  }

  @Override
  public boolean isLoggable(Level level) {
    if (level == Level.FINEST) {
      return (alwaysActive || loggingActive.get()) &&
          (samplingRate >= 1.0d || (samplingRate > 0.0d && RANDOM.nextDouble() < samplingRate));
    } else {
      return delegate.isLoggable(level);
    }
  }

  /**
   * Checks the logger state and writes the message in the log if appropriate.
   * We log valid points only if the system property wavefront.proxy.logpoints is true
   * (for legacy reasons) or the delegate logger's log level is set to "ALL"
   * (i.e. if Level.FINEST is considered loggable). This is done to prevent introducing
   * additional overhead into the critical path, as well as prevent accidentally logging points
   * into the main log. Additionally, honor sample rate limit, if set.
   *
   * @param level   log level.
   * @param message string to write to log.
   */
  private void sampleAndLog(Level level, String message) {
      if ((alwaysActive || loggingActive.get()) &&
        (samplingRate >= 1.0d || (samplingRate > 0.0d && RANDOM.nextDouble() < samplingRate))) {
      delegate.log(level, message);
    }
  }

  private void refreshLoggerState() {
    if (loggingActive.get() != delegate.isLoggable(Level.FINEST)) {
      loggingActive.set(!loggingActive.get());
      logger.info("Valid " + entityType.toString() + " logging is now " + (loggingActive.get() ?
          "enabled with " + (samplingRate * 100) + "% sampling" : "disabled"));
    }
  }
}
