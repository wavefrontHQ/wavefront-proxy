package com.wavefront.agent.listeners.tracing;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.listeners.PortUnificationHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportableEntityDecoder;
import com.wavefront.sdk.entities.tracing.sampling.Sampler;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.channel.ChannelHandlerContext;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_KEY;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;

/**
 * Process incoming trace-formatted data.
 *
 * Accepts incoming messages of either String or FullHttpRequest type: single Span in a string,
 * or multiple points in the HTTP post body, newline-delimited.
 *
 * @author vasily@wavefront.com
 */
public class TracePortUnificationHandler extends PortUnificationHandler {
  private static final Logger logger = Logger.getLogger(
      TracePortUnificationHandler.class.getCanonicalName());

  private static final ObjectMapper JSON_PARSER = new ObjectMapper();

  private final ReportableEntityHandler<Span> handler;
  private final ReportableEntityHandler<SpanLogs> spanLogsHandler;
  private final ReportableEntityDecoder<String, Span> decoder;
  private final ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDecoder;
  private final ReportableEntityPreprocessor preprocessor;
  private final Sampler sampler;
  private final boolean alwaysSampleErrors;

  @SuppressWarnings("unchecked")
  public TracePortUnificationHandler(final String handle,
                                     final TokenAuthenticator tokenAuthenticator,
                                     final ReportableEntityDecoder<String, Span> traceDecoder,
                                     final ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDecoder,
                                     @Nullable final ReportableEntityPreprocessor preprocessor,
                                     final ReportableEntityHandlerFactory handlerFactory,
                                     final Sampler sampler,
                                     final boolean alwaysSampleErrors) {
    this(handle, tokenAuthenticator, traceDecoder, spanLogsDecoder, preprocessor,
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)),
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE_SPAN_LOGS, handle)),
        sampler, alwaysSampleErrors);
  }

  public TracePortUnificationHandler(final String handle,
                                     final TokenAuthenticator tokenAuthenticator,
                                     final ReportableEntityDecoder<String, Span> traceDecoder,
                                     final ReportableEntityDecoder<JsonNode, SpanLogs> spanLogsDecoder,
                                     @Nullable final ReportableEntityPreprocessor preprocessor,
                                     final ReportableEntityHandler<Span> handler,
                                     final ReportableEntityHandler<SpanLogs> spanLogsHandler,
                                     final Sampler sampler,
                                     final boolean alwaysSampleErrors) {
    super(tokenAuthenticator, handle, true, true);
    this.decoder = traceDecoder;
    this.spanLogsDecoder = spanLogsDecoder;
    this.handler = handler;
    this.spanLogsHandler = spanLogsHandler;
    this.preprocessor = preprocessor;
    this.sampler = sampler;
    this.alwaysSampleErrors = alwaysSampleErrors;
  }

  @Override
  protected void processLine(final ChannelHandlerContext ctx, @Nonnull String message) {
    if (message.startsWith("{") && message.endsWith("}")) { // span logs
      try {
        List<SpanLogs> output = Lists.newArrayListWithCapacity(1);
        spanLogsDecoder.decode(JSON_PARSER.readTree(message), output, "dummy");
        for (SpanLogs object : output) {
          spanLogsHandler.report(object);
        }
      } catch (Exception e) {
        spanLogsHandler.reject(message, parseError(message, ctx, e));
      }
      return;
    }

    // transform the line if needed
    if (preprocessor != null) {
      message = preprocessor.forPointLine().transform(message);

      // apply white/black lists after formatting
      if (!preprocessor.forPointLine().filter(message)) {
        if (preprocessor.forPointLine().getLastFilterResult() != null) {
          handler.reject((Span) null, message);
        } else {
          handler.block(null, message);
        }
        return;
      }
    }

    List<Span> output = Lists.newArrayListWithCapacity(1);
    try {
      decoder.decode(message, output, "dummy");
    } catch (Exception e) {
      handler.reject(message, parseError(message, ctx, e));
      return;
    }

    for (Span object : output) {
      if (preprocessor != null) {
        preprocessor.forSpan().transform(object);
        if (!preprocessor.forSpan().filter((object))) {
          if (preprocessor.forSpan().getLastFilterResult() != null) {
            handler.reject(object, preprocessor.forSpan().getLastFilterResult());
          } else {
            handler.block(object);
          }
          return;
        }
      }
      boolean sampleError = false;
      if (alwaysSampleErrors) {
        // check whether error span tag exists.
        sampleError = object.getAnnotations().stream().anyMatch(
            t -> t.getKey().equals(ERROR_SPAN_TAG_KEY) && t.getValue().equals(ERROR_SPAN_TAG_VAL));
      }
      if (sampleError || sampler.sample(object.getName(),
          UUID.fromString(object.getTraceId()).getLeastSignificantBits(), object.getDuration())) {
        handler.report(object);
      }
    }
  }

  private static String parseError(String message, @Nullable ChannelHandlerContext ctx, @Nonnull Throwable e) {
    final Throwable rootCause = Throwables.getRootCause(e);
    StringBuilder errMsg = new StringBuilder("WF-300 Cannot parse: \"");
    errMsg.append(message);
    errMsg.append("\", reason: \"");
    errMsg.append(e.getMessage());
    errMsg.append("\"");
    if (rootCause != null && rootCause.getMessage() != null && rootCause != e) {
      errMsg.append(", root cause: \"");
      errMsg.append(rootCause.getMessage());
      errMsg.append("\"");
    }
    if (ctx != null) {
      InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
      if (remoteAddress != null) {
        errMsg.append("; remote: ");
        errMsg.append(remoteAddress.getHostString());
      }
    }
    return errMsg.toString();
  }
}
