package com.wavefront.agent.listeners;

import static com.wavefront.agent.channel.ChannelUtils.errorMessageWithRootCause;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static com.wavefront.agent.formatter.DataFormat.LOGS_JSON_ARR;
import static com.wavefront.agent.formatter.DataFormat.LOGS_JSON_LINES;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.formatter.DataFormat;
import com.wavefront.common.Utils;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.jetbrains.annotations.NotNull;

/**
 * Base class for all line-based protocols. Supports TCP line protocol as well as HTTP POST with
 * newline-delimited payload.
 */
@ChannelHandler.Sharable
public abstract class AbstractLineDelimitedHandler extends AbstractPortUnificationHandler {

  public static final ObjectMapper JSON_PARSER = new ObjectMapper();
  private final Supplier<Histogram> receivedLogsBatches;

  /**
   * @param tokenAuthenticator {@link TokenAuthenticator} for incoming requests.
   * @param healthCheckManager shared health check endpoint handler.
   * @param port handle/port number.
   */
  public AbstractLineDelimitedHandler(
      @Nullable final TokenAuthenticator tokenAuthenticator,
      @Nullable final HealthCheckManager healthCheckManager,
      final int port) {
    super(tokenAuthenticator, healthCheckManager, port);
    this.receivedLogsBatches =
        Utils.lazySupplier(
            () -> Metrics.newHistogram(new MetricName("logs." + port, "", "received.batches")));
  }

  /** Handles an incoming HTTP message. Accepts HTTP POST on all paths */
  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx, final FullHttpRequest request) {
    StringBuilder output = new StringBuilder();
    HttpResponseStatus status;
    try {
      DataFormat format = getFormat(request);
      processBatchMetrics(ctx, request, format);
      // Log batches may contain new lines as part of the message payload so we
      // special case
      // handling breaking up the batches
      Iterable<String> lines;

      if (format == LOGS_JSON_ARR) {
        lines = extractLogsWithJsonArrayFormat(request);
      } else if (format == LOGS_JSON_LINES) {
        lines = extractLogsWithJsonLinesFormat(request);
      } else {
        lines = extractLogsWithDefaultFormat(request);
      }

      lines.forEach(line -> processLine(ctx, line, format));
      status = HttpResponseStatus.ACCEPTED;
    } catch (Exception e) {
      status = HttpResponseStatus.BAD_REQUEST;
      output.append(errorMessageWithRootCause(e));
      logWarning("WF-300: Failed to handle HTTP POST", e, ctx);
    }
    writeHttpResponse(ctx, status, output, request);
  }

  private Iterable<String> extractLogsWithDefaultFormat(FullHttpRequest request) {
    return Splitter.on('\n')
        .trimResults()
        .omitEmptyStrings()
        .split(request.content().toString(CharsetUtil.UTF_8));
  }

  private Iterable<String> extractLogsWithJsonLinesFormat(FullHttpRequest request)
      throws IOException {
    List<String> lines = new ArrayList<>();
    MappingIterator<JsonNode> it =
        JSON_PARSER
            .readerFor(JsonNode.class)
            .readValues(request.content().toString(CharsetUtil.UTF_8));
    while (it.hasNextValue()) {
      lines.add(JSON_PARSER.writeValueAsString(it.nextValue()));
    }
    return lines;
  }

  @NotNull
  private static Iterable<String> extractLogsWithJsonArrayFormat(FullHttpRequest request)
      throws IOException {
    return JSON_PARSER
        .readValue(
            request.content().toString(CharsetUtil.UTF_8),
            new TypeReference<List<Map<String, Object>>>() {})
        .stream()
        .map(
            json -> {
              try {
                return JSON_PARSER.writeValueAsString(json);
              } catch (JsonProcessingException e) {
                return null;
              }
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /**
   * Handles an incoming plain text (string) message. By default simply passes a string to {@link
   * #processLine(ChannelHandlerContext, String, DataFormat)} method.
   */
  @Override
  protected void handlePlainTextMessage(
      final ChannelHandlerContext ctx, @Nonnull final String message) {
    String trimmedMessage = message.trim();
    if (trimmedMessage.isEmpty()) return;
    processLine(ctx, trimmedMessage, null);
  }

  /**
   * Detect data format for an incoming HTTP request, if possible.
   *
   * @param httpRequest http request.
   * @return Detected data format or null if unknown.
   */
  @Nullable
  protected abstract DataFormat getFormat(FullHttpRequest httpRequest);

  /**
   * Process a single line for a line-based stream.
   *
   * @param ctx Channel handler context.
   * @param message Message to process.
   * @param format Data format, if known
   */
  protected abstract void processLine(
      final ChannelHandlerContext ctx, @Nonnull final String message, @Nullable DataFormat format);

  protected void processBatchMetrics(
      final ChannelHandlerContext ctx, final FullHttpRequest request, @Nullable DataFormat format) {
    if (format == LOGS_JSON_ARR || format == LOGS_JSON_LINES) {
      receivedLogsBatches.get().update(request.content().toString(CharsetUtil.UTF_8).length());
    }
  }
}
