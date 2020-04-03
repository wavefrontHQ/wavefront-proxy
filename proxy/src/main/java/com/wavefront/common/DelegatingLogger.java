package com.wavefront.common;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Base class for delegating loggers.
 *
 * @author vasily@wavefront.com
 */
public abstract class DelegatingLogger extends Logger {
  protected final Logger delegate;

  /**
   * @param delegate     Delegate logger.
   */
  public DelegatingLogger(Logger delegate) {
    super(delegate.getName(), null);
    this.delegate = delegate;
  }

  /**
   * @param level   log level.
   * @param message string to write to log.
   */
  @Override
  public abstract void log(Level level, String message);

  /**
   * @param logRecord log record to write to log.
   */
  @Override
  public void log(LogRecord logRecord) {
    logRecord.setLoggerName(delegate.getName());
    inferCaller(logRecord);
    delegate.log(logRecord);
  }

  private void inferCaller(LogRecord logRecord) {
    Optional<StackWalker.StackFrame> frame = new CallerFinder().get();
    frame.ifPresent(f -> {
      logRecord.setSourceClassName(f.getClassName());
      logRecord.setSourceMethodName(f.getMethodName());
    });
  }

  static final class CallerFinder implements Predicate<StackWalker.StackFrame> {
    private static final StackWalker WALKER;
    static {
      final PrivilegedAction<StackWalker> action =
          () -> StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
      WALKER = AccessController.doPrivileged(action);
    }

    Optional<StackWalker.StackFrame> get() {
      return WALKER.walk(s -> s.filter(this).findFirst());
    }

    private boolean lookingForLogger = true;

    @Override
    public boolean test(StackWalker.StackFrame t) {
      final String cname = t.getClassName();
      if (lookingForLogger) {
        // the log record could be created for a platform logger
        lookingForLogger = !(cname.endsWith("Logger") ||
            cname.startsWith("sun.util.logging.PlatformLogger"));
        return false;
      }
      return !cname.endsWith("Logger");
    }
  }
}
