package com.wavefront.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.wavefront.data.ReportableEntityType;

import javax.annotation.Nullable;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A sampling logger that can be enabled and disabled dynamically
 * by setting its level to {@code Level.FINEST}.
 *
 * @author vasily@wavefront.com
 */
public class SamplingLogger extends DelegatingLogger {
  private static final Random RANDOM = new Random();

  private final ReportableEntityType entityType;
  private final double samplingRate;
  private final boolean alwaysActive;
  private final Consumer<String> statusChangeConsumer;
  private final AtomicBoolean loggingActive = new AtomicBoolean(false);

  /**
   * @param entityType   entity type (used in info messages only).
   * @param delegate     delegate logger name.
   * @param samplingRate sampling rate for logging [0..1].
   * @param alwaysActive whether this logger is always active regardless of currently set log level.
   */
  public SamplingLogger(ReportableEntityType entityType,
                        Logger delegate,
                        double samplingRate,
                        boolean alwaysActive,
                        @Nullable Consumer<String> statusChangeConsumer) {
    super(delegate);
    Preconditions.checkArgument(samplingRate >= 0, "Sampling rate should be positive!");
    Preconditions.checkArgument(samplingRate <= 1, "Sampling rate should not be be > 1!");
    this.entityType = entityType;
    this.samplingRate = samplingRate;
    this.alwaysActive = alwaysActive;
    this.statusChangeConsumer = statusChangeConsumer;
    refreshLoggerState();
    new Timer("Timer-sampling-logger-" + delegate.getName()).scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        refreshLoggerState();
      }
    }, 1000, 1000);
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
  @Override
  public void log(Level level, String message) {
      if ((alwaysActive || loggingActive.get()) &&
        (samplingRate >= 1.0d || (samplingRate > 0.0d && RANDOM.nextDouble() < samplingRate))) {
      log(new LogRecord(level, message));
    }
  }

  @VisibleForTesting
  void refreshLoggerState() {
    boolean finestLoggable = delegate.isLoggable(Level.FINEST);
    if (loggingActive.compareAndSet(!finestLoggable, finestLoggable)) {
      if (statusChangeConsumer != null) {
        String status = loggingActive.get() ?
            "enabled with " + (samplingRate * 100) + "% sampling" :
            "disabled";
        statusChangeConsumer.accept("Valid " + entityType.toString() + " logging is now " + status);
      }
    }
  }
}
