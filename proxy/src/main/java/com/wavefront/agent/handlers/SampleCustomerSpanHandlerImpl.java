package com.wavefront.agent.handlers;

import java.util.Collection;

import wavefront.report.Span;

public class SampleCustomerSpanHandlerImpl extends SpanHandlerImpl {
  SampleCustomerSpanHandlerImpl(final String handle,
                                final int blockedItemsPerBatch,
                                final Collection<SenderTask> senderTasks) {
    super(handle, blockedItemsPerBatch, senderTasks);
  }

  @Override
  protected void reportInternal(Span span) {
    span.setCustomer("sample");
    super.reportInternal(span);
  }
}
