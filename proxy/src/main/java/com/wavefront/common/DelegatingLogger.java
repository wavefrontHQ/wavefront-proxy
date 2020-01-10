package com.wavefront.common;

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

  /**
   * This is a JDK8-specific implementation. TODO: switch to StackWalker after migrating to JDK9+
   */
  private void inferCaller(LogRecord logRecord) {
    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
    boolean lookingForLogger = true;
    for (StackTraceElement frame : stackTraceElements) {
      String cname = frame.getClassName();
      if (lookingForLogger) {
        // Skip all frames until we have found the first logger frame.
        if (cname.endsWith("Logger")) {
          lookingForLogger = false;
        }
      } else {
        if (!cname.endsWith("Logger") && !cname.startsWith("java.lang.reflect.") &&
            !cname.startsWith("sun.reflect.")) {
          // We've found the relevant frame.
          logRecord.setSourceClassName(cname);
          logRecord.setSourceMethodName(frame.getMethodName());
          return;
        }
      }
    }
  }
}
