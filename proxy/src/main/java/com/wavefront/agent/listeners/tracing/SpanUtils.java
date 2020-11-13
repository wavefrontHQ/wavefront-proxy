package com.wavefront.agent.listeners.tracing;

import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.ingester.ReportableEntityDecoder;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandlerContext;
import wavefront.report.Annotation;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

import static com.wavefront.agent.channel.ChannelUtils.formatErrorMessage;

/**
 * Utility methods for handling Span and SpanLogs.
 *
 * @author Shipeng Xie (xshipeng@vmware.com)
 */
public final class SpanUtils {
  private static final Logger logger = Logger.getLogger(
      SpanUtils.class.getCanonicalName());
  private static final ObjectMapper JSON_PARSER = new ObjectMapper();
  private final static String FORCE_SAMPLED_KEY = "sampling.priority";

  private SpanUtils() {
  }

  /**
   * Preprocess and handle span.
   *
   * @param message              encoded span data.
   * @param decoder              span decoder.
   * @param handler              span handler.
   * @param spanReporter         span reporter.
   * @param preprocessorSupplier span preprocessor.
   * @param ctx                  channel handler context.
   * @param samplerFunc          span sampler.
   */
  public static void preprocessAndHandleSpan(
      String message, ReportableEntityDecoder<String, Span> decoder,
      ReportableEntityHandler<Span, String> handler, Consumer<Span> spanReporter,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      @Nullable ChannelHandlerContext ctx, Function<Span, Boolean> samplerFunc) {
    ReportableEntityPreprocessor preprocessor = preprocessorSupplier == null ?
        null : preprocessorSupplier.get();
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
      if (isForceSampled(object) || samplerFunc.apply(object)) {
        spanReporter.accept(object);
      }
    }
  }

  /**
   * Handle spanLogs.
   *
   * @param message              encoded spanLogs data.
   * @param spanLogsDecoder      spanLogs decoder.
   * @param spanDecoder          span decoder.
   * @param handler              spanLogs handler.
   * @param preprocessorSupplier spanLogs preprocessor.
   * @param ctx                  channel handler context.
   * @param samplerFunc          span sampler.
   */
  public static void handleSpanLogs(
      String message, ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDecoder,
      ReportableEntityDecoder<String, Span> spanDecoder,
      ReportableEntityHandler<SpanLogs, String> handler,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      @Nullable ChannelHandlerContext ctx, Function<Span, Boolean> samplerFunc) {
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
        ReportableEntityPreprocessor preprocessor = preprocessorSupplier == null ?
            null : preprocessorSupplier.get();
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
          if (isForceSampled(span) || samplerFunc.apply(span)) {
            // after sampling, span line data is no longer needed
            spanLogs.setSpan(null);
            handler.report(spanLogs);
          }
        }
      }
    }
  }

  @VisibleForTesting
  static boolean isForceSampled(Span span) {
    List<Annotation> annotations = span.getAnnotations();
    for (Annotation annotation : annotations) {
      if (annotation.getKey().equals(FORCE_SAMPLED_KEY)) {
        try {
          if (NumberFormat.getInstance().parse(annotation.getValue()).doubleValue() > 0) {
            return true;
          }
        } catch (ParseException e) {
          if (logger.isLoggable(Level.FINE)) {
            logger.info("Invalid value :: " + annotation.getValue() +
                " for span tag key : " + FORCE_SAMPLED_KEY + " for span : " + span.getName());
          }
        }
      }
    }
    return false;
  }

}
