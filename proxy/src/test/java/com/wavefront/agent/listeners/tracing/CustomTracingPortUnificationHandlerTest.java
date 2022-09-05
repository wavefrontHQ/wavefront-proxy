package com.wavefront.agent.listeners.tracing;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.captureLong;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.internal_reporter_java.io.dropwizard.metrics5.DeltaCounter;
import com.wavefront.internal_reporter_java.io.dropwizard.metrics5.WavefrontHistogram;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.Span;

public class CustomTracingPortUnificationHandlerTest {
  @Test
  public void reportsCorrectDuration() {
    WavefrontInternalReporter reporter = EasyMock.niceMock(WavefrontInternalReporter.class);
    expect(reporter.newDeltaCounter(anyObject())).andReturn(new DeltaCounter()).anyTimes();
    WavefrontHistogram histogram = EasyMock.niceMock(WavefrontHistogram.class);
    expect(reporter.newWavefrontHistogram(anyObject(), anyObject())).andReturn(histogram);
    Capture<Long> duration = newCapture();
    histogram.update(captureLong(duration));
    expectLastCall();
    ReportableEntityHandler<Span, String> handler =
        MockReportableEntityHandlerFactory.getMockTraceHandler();
    CustomTracingPortUnificationHandler subject =
        new CustomTracingPortUnificationHandler(
            null, null, null, null, null, null, handler, null, null, null, null, null, reporter,
            null, null, null);
    replay(reporter, histogram);

    Span span = getSpan();
    span.setDuration(1000); // milliseconds
    subject.report(span);
    verify(reporter, histogram);
    long value = duration.getValue();
    assertEquals(1000000, value); // microseconds
  }

  @NotNull
  private Span getSpan() {
    Span span = new Span();
    span.setAnnotations(
        ImmutableList.of(
            new Annotation("application", "application"), new Annotation("service", "service")));
    return span;
  }
}
