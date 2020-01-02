package com.wavefront.agent.handlers;

import javax.annotation.Nonnull;

/**
 * Wrapper for {@link ReportableEntityHandlerFactory} to allow partial overrides for the
 * {@code getHandler} method.
 *
 * @author vasily@wavefront.com
 */
public class DelegatingReportableEntityHandlerFactoryImpl implements ReportableEntityHandlerFactory {
  protected final ReportableEntityHandlerFactory delegate;

  public DelegatingReportableEntityHandlerFactoryImpl(ReportableEntityHandlerFactory delegate) {
    this.delegate = delegate;
  }

  @Override
  public <T, U> ReportableEntityHandler<T, U> getHandler(HandlerKey handlerKey) {
    return delegate.getHandler(handlerKey);
  }

  @Override
  public void shutdown(@Nonnull String handle) {
    delegate.shutdown(handle);
  }
}
