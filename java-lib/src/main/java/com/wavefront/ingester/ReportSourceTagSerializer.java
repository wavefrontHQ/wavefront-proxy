package com.wavefront.ingester;

import java.util.function.Function;

import wavefront.report.ReportSourceTag;

/**
 * Convert a {@link ReportSourceTag} to its string representation.
 *
 * @author vasily@wavefront.com
 */
public class ReportSourceTagSerializer implements Function<ReportSourceTag, String> {
  @Override
  public String apply(ReportSourceTag input) {
    return input.toString();
  }
}
