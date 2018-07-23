package com.wavefront.ingester;

import java.util.List;

import javax.validation.constraints.NotNull;

import wavefront.report.ReportPoint;

/**
 * Wraps {@link Decoder} as {@link ReportableEntityDecoder}.
 *
 * @author vasily@wavefront.com
 */
public class ReportPointDecoderWrapper implements ReportableEntityDecoder<String, ReportPoint> {

  private final Decoder<String> delegate;

    public ReportPointDecoderWrapper(@NotNull Decoder<String> delegate) {
      this.delegate = delegate;
    }

    @Override
    public void decode(String msg, List<ReportPoint> out, String customerId) {
      delegate.decodeReportPoints(msg, out, customerId);
    }
}
