package com.wavefront.agent.listeners;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import wavefront.report.Span;

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
public class OtlpUtils {
  /**
   * OpenTelemetry Span/Trace ID length specification ref: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#spancontext
   *
   * TraceId A valid trace identifier is a 16-byte array with at least one non-zero byte.
   * SpanId A valid span identifier is an 8-byte array with at least one non-zero byte.
   */
  private static final int TRACE_ID_BYTE_LENGTH = 16;
  private static final int SPAN_ID_BYTE_LENGTH = 8;

  private final static Logger logger = Logger.getLogger(OtlpUtils.class.getCanonicalName());

  public static List<Span> otlpSpanExportRequestParseToWFSpan(ExportTraceServiceRequest request) {
    List<Span> wfSpans = Lists.newArrayList();
    for (ResourceSpans resourceSpans : request.getResourceSpansList()) {
      for (InstrumentationLibrarySpans instrumentationLibrarySpans :
          resourceSpans.getInstrumentationLibrarySpansList()) {
        for (io.opentelemetry.proto.trace.v1.Span otlpSpan : instrumentationLibrarySpans.getSpansList()) {
          // TODO: the spanid and traceid format (UUID) need further exploration
          UUID wfSpanId = getUUIDFromBytes(otlpSpan.getSpanId());
          UUID wfTraceId = getUUIDFromBytes(otlpSpan.getTraceId());

          wavefront.report.Span wfSpan = wavefront.report.Span.newBuilder().
              setName(otlpSpan.getName()).
              setSpanId(wfSpanId.toString()).
              setTraceId(wfTraceId.toString()).
              setStartMillis(otlpSpan.getStartTimeUnixNano() / 1000).
              setDuration((otlpSpan.getEndTimeUnixNano() - otlpSpan.getStartTimeUnixNano()) / 1000).
              setSource("open-telemetry").
              setCustomer("wf-proxy").build();
          logger.info("Transformed OTLP into WF span: " + wfSpan);
          wfSpans.add(wfSpan);
        }
      }
    }
    return wfSpans;
  }

  // TODO: test and handle case of invalid spans (e.g. increment metric indicating
  //  invalid or blocked or dropped span
  private static UUID getUUIDFromBytes(ByteString byteString) throws IllegalArgumentException {
    byte[] bytes = byteString.toByteArray();

    if (bytes.length == SPAN_ID_BYTE_LENGTH) {
      ByteBuffer part1 = ByteBuffer.allocate(bytes.length);
      part1.put(bytes);
      return new UUID(0L, part1.getLong(0));
    } else if (bytes.length == TRACE_ID_BYTE_LENGTH) {
      ByteBuffer part1 = ByteBuffer.allocate(bytes.length / 2);
      part1.put(bytes, 0, bytes.length / 2);
      ByteBuffer part2 = ByteBuffer.allocate(bytes.length / 2);
      part2.put(bytes, bytes.length / 2, bytes.length / 2);
      return new UUID(part1.getLong(0), part2.getLong(0));
    }
    throw new IllegalArgumentException("Invalid Span/Trace ID bytes received.");
  }
}
