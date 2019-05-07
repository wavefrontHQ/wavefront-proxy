package com.wavefront.agent.handlers;

import java.util.Collection;

import wavefront.report.ReportPoint;

public class SampleCustomerReportPointHandlerImpl extends ReportPointHandlerImpl {
  SampleCustomerReportPointHandlerImpl(final String handle,
                                       final int blockedItemsPerBatch,
                                       final Collection<SenderTask> senderTasks) {
    super(handle, blockedItemsPerBatch, senderTasks);
  }

  @Override
  void reportInternal(ReportPoint point) {
    point.setTable("sample");
    super.reportInternal(point);
  }
}
