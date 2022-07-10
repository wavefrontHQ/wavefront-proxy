package com.wavefront.agent.listeners.tracing;

import static com.wavefront.agent.channel.ChannelUtils.formatErrorMessage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.wavefront.agent.core.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.ingester.ReportableEntityDecoder;
import io.netty.channel.ChannelHandlerContext;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

/**
 * Utility methods for handling Span and SpanLogs.
 *
 * @author Shipeng Xie (xshipeng@vmware.com)
 */
public final class SpanUtils {
  private static final Logger logger = Logger.getLogger(SpanUtils.class.getCanonicalName());
  private static final ObjectMapper JSON_PARSER = new ObjectMapper();

  private SpanUtils() {}

  /**
   * Preprocess and handle span.
   *
   * @param message encoded span data.
   * @param decoder span decoder.
   * @param handler span handler.
   * @param spanReporter span reporter.
   * @param preprocessorSupplier span preprocessor.
   * @param ctx channel handler context.
   * @param samplerFunc span sampler.
   */
  public static void preprocessAndHandleSpan(
      String message,
      ReportableEntityDecoder<String, Span> decoder,
      ReportableEntityHandler<Span> handler,
      Consumer<Span> spanReporter,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      @Nullable ChannelHandlerContext ctx,
      Function<Span, Boolean> samplerFunc) {
    ReportableEntityPreprocessor preprocessor =
        preprocessorSupplier == null ? null : preprocessorSupplier.get();
    String[] messageHolder = new String[1];

    // transform the line if needed
    if (preprocessor != null) {
      message = preprocessor.forPointLine().transform(message);

      if (!preprocessor.forPointLine().filter(message, messageHolder)) {
        if (messageHolder[0] != null) {
          handler.reject((Span) null, messageHolder[0]);
        } else {
          handler.block(null, message);
        }
        return;
      }
    }
    List<Span> output = new ArrayList<>(1);
    try {
      decoder.decode(message, output, "dummy");
    } catch (Exception e) {
      handler.reject(message, formatErrorMessage(message, e, ctx));
      return;
    }

    for (Span object : output) {
      if (preprocessor != null) {
        preprocessor.forSpan().transform(object);
        if (!preprocessor.forSpan().filter(object, messageHolder)) {
          if (messageHolder[0] != null) {
            handler.reject(object, messageHolder[0]);
          } else {
            handler.block(object);
          }
          return;
        }
      }
      if (samplerFunc.apply(object)) {
        spanReporter.accept(object);
      }
    }
  }

  /**
   * Handle spanLogs.
   *
   * @param message encoded spanLogs data.
   * @param spanLogsDecoder spanLogs decoder.
   * @param spanDecoder span decoder.
   * @param handler spanLogs handler.
   * @param preprocessorSupplier spanLogs preprocessor.
   * @param ctx channel handler context.
   * @param samplerFunc span sampler.
   */
  public static void handleSpanLogs(
      String message,
      ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDecoder,
      ReportableEntityDecoder<String, Span> spanDecoder,
      ReportableEntityHandler<SpanLogs> handler,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      @Nullable ChannelHandlerContext ctx,
      Function<Span, Boolean> samplerFunc) {
    List<SpanLogs> spanLogsOutput = new ArrayList<>(1);
    try {
      spanLogsDecoder.decode(JSON_PARSER.readTree(message), spanLogsOutput, "dummy");
    } catch (Exception e) {
      handler.reject(message, formatErrorMessage(message, e, ctx));
      return;
    }

    for (SpanLogs spanLogs : spanLogsOutput) {
      String spanMessage = spanLogs.getSpan();
      if (spanMessage == null) {
        // For backwards compatibility, report the span logs if span line data is not
        // included
        handler.report(spanLogs);
      } else {
        ReportableEntityPreprocessor preprocessor =
            preprocessorSupplier == null ? null : preprocessorSupplier.get();
        String[] spanMessageHolder = new String[1];

        // transform the line if needed
        if (preprocessor != null) {
          spanMessage = preprocessor.forPointLine().transform(spanMessage);

          if (!preprocessor.forPointLine().filter(message, spanMessageHolder)) {
            if (spanMessageHolder[0] != null) {
              handler.reject(spanLogs, spanMessageHolder[0]);
            } else {
              handler.block(spanLogs);
            }
            return;
          }
        }
        List<Span> spanOutput = new ArrayList<>(1);
        try {
          spanDecoder.decode(spanMessage, spanOutput, "dummy");
        } catch (Exception e) {
          handler.reject(spanLogs, formatErrorMessage(message, e, ctx));
          return;
        }

        if (!spanOutput.isEmpty()) {
          Span span = spanOutput.get(0);
          if (preprocessor != null) {
            preprocessor.forSpan().transform(span);
            if (!preprocessor.forSpan().filter(span, spanMessageHolder)) {
              if (spanMessageHolder[0] != null) {
                handler.reject(spanLogs, spanMessageHolder[0]);
              } else {
                handler.block(spanLogs);
              }
              return;
            }
          }
          if (samplerFunc.apply(span)) {
            // after sampling, span line data is no longer needed
            spanLogs.setSpan(null);
            handler.report(spanLogs);
          }
        }
      }
    }
  }

  public static String toStringId(ByteString id) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(id.toByteArray());
    long mostSigBits = id.toByteArray().length > 8 ? byteBuffer.getLong() : 0L;
    long leastSigBits = new BigInteger(1, byteBuffer.array()).longValue();
    UUID uuid = new UUID(mostSigBits, leastSigBits);
    return uuid.toString();
  }
}
