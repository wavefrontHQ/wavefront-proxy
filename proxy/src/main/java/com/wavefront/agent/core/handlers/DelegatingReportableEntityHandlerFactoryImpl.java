package com.wavefront.agent.core.handlers;

import com.wavefront.agent.core.queues.QueueInfo;
import javax.annotation.Nonnull;

/**
 * Wrapper for {@link ReportableEntityHandlerFactory} to allow partial overrides for the {@code
 * getHandler} method.
 */
public class DelegatingReportableEntityHandlerFactoryImpl
    implements ReportableEntityHandlerFactory {
  protected final ReportableEntityHandlerFactory delegate;

  public DelegatingReportableEntityHandlerFactoryImpl(ReportableEntityHandlerFactory delegate) {
    this.delegate = delegate;
  }

  @Override
  public <T> ReportableEntityHandler<T> getHandler(String handler, QueueInfo queue) {
    return delegate.getHandler(handler, queue);
  }

  @Override
  public void shutdown(int handle) {
    delegate.shutdown(handle);
  }
}
