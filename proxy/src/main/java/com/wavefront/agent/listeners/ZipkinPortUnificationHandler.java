package com.wavefront.agent.listeners;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

import com.wavefront.agent.Utils;
import com.wavefront.agent.auth.TokenAuthenticatorBuilder;
import com.wavefront.agent.auth.TokenValidationMethod;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.TraceConstants;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import wavefront.report.Annotation;
import wavefront.report.Span;
import zipkin2.SpanBytesDecoderDetector;
import zipkin2.codec.BytesDecoder;

import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SOURCE_KEY;

/**
 * Handler that processes Zipkin trace data over HTTP and converts them to Wavefront format.
 *
 * @author Anil Kodali (akodali@vmware.com)
 */
public class ZipkinPortUnificationHandler extends PortUnificationHandler {
  private static final Logger logger = Logger.getLogger(
      ZipkinPortUnificationHandler.class.getCanonicalName());
  private final String handle;
  private final ReportableEntityHandler<Span> handler;
  private final AtomicBoolean traceDisabled;
  private final ReportableEntityPreprocessor preprocessor;
  private final RateLimiter warningLoggerRateLimiter = RateLimiter.create(0.2);
  private final Counter discardedBatches;
  private final Counter processedBatches;
  private final Counter failedBatches;

  private final static Set<String> ZIPKIN_VALID_PATHS = ImmutableSet.of(
      "/api/v1/spans/",
      "/api/v2/spans/");
  private final static String ZIPKIN_VALID_HTTP_METHOD = "POST";
  private final static String DEFAULT_APPLICATION = "Zipkin";
  private final static String DEFAULT_SOURCE = "zipkin";
  private final static String DEFAULT_SERVICE = "defaultService";
  private final static String DEFAULT_SPAN_NAME = "defaultOperation";

  private static final Logger zipkinDataLogger = Logger.getLogger("ZipkinDataLogger");

  public ZipkinPortUnificationHandler(String handle,
                                      ReportableEntityHandlerFactory handlerFactory,
                                      AtomicBoolean traceDisabled,
                                      @Nullable ReportableEntityPreprocessor preprocessor) {
    this(handle,
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)),
        traceDisabled, preprocessor);
  }

  public ZipkinPortUnificationHandler(final String handle,
                                      ReportableEntityHandler<Span> handler,
                                      AtomicBoolean traceDisabled,
                                      @Nullable ReportableEntityPreprocessor preprocessor) {
    super(TokenAuthenticatorBuilder.create().setTokenValidationMethod(TokenValidationMethod.NONE).build(),
        handle, false, true);
    this.handle = handle;
    this.handler = handler;
    this.traceDisabled = traceDisabled;
    this.preprocessor = preprocessor;
    this.discardedBatches = Metrics.newCounter(new MetricName(
        "spans." + handle + ".batches", "", "discarded"));
    this.processedBatches = Metrics.newCounter(new MetricName(
        "spans." + handle + ".batches", "", "processed"));
    this.failedBatches = Metrics.newCounter(new MetricName(
        "spans." + handle + ".batches", "", "failed"));
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest incomingRequest) {
    URI uri = parseUri(ctx, incomingRequest);
    if (uri == null) return;

    String path = uri.getPath().endsWith("/") ? uri.getPath() : uri.getPath() + "/";

    // Validate Uri Path and HTTP method of incoming Zipkin spans.
    if (!ZIPKIN_VALID_PATHS.contains(path)) {
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, "Unsupported URL path.", incomingRequest);
      logWarning("WF-400: Requested URI path '" + path + "' is not supported.", null, ctx);
      return;
    }
    if (!incomingRequest.method().toString().equalsIgnoreCase(ZIPKIN_VALID_HTTP_METHOD)) {
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, "Unsupported Http method.", incomingRequest);
      logWarning("WF-400: Requested http method '" + incomingRequest.method().toString() +
          "' is not supported.", null, ctx);
      return;
    }

    HttpResponseStatus status;
    StringBuilder output = new StringBuilder();

    // Handle case when tracing is disabled, ignore reported spans.
    if (traceDisabled.get()) {
      if (warningLoggerRateLimiter.tryAcquire()) {
        logger.info("Ingested spans discarded because tracing feature is not enabled on the " +
            "server");
      }
      discardedBatches.inc();
      output.append("Ingested spans discarded because tracing feature is not enabled on the " +
          "server.");
      status = HttpResponseStatus.ACCEPTED;
      writeHttpResponse(ctx, status, output, incomingRequest);
      return;
    }

    try {
      byte[] bytesArray = new byte[incomingRequest.content().nioBuffer().remaining()];
      incomingRequest.content().nioBuffer().get(bytesArray, 0, bytesArray.length);
      BytesDecoder<zipkin2.Span> decoder = SpanBytesDecoderDetector.decoderForListMessage(bytesArray);
      List<zipkin2.Span> zipkinSpanSink = new ArrayList<>();
      decoder.decodeList(bytesArray, zipkinSpanSink);
      processZipkinSpans(zipkinSpanSink);
      status = HttpResponseStatus.ACCEPTED;
      processedBatches.inc();
    } catch (Exception e) {
      failedBatches.inc();
      writeExceptionText(e, output);
      status = HttpResponseStatus.BAD_REQUEST;
      logger.log(Level.WARNING, "Zipkin batch processing failed", Throwables.getRootCause(e));
    }
    writeHttpResponse(ctx, status, output, incomingRequest);
  }

  private void processZipkinSpans(List<zipkin2.Span> zipkinSpans) {
    for (zipkin2.Span zipkinSpan : zipkinSpans) {
      processZipkinSpan(zipkinSpan);
    }
  }

  private void processZipkinSpan(zipkin2.Span zipkinSpan) {
    if (zipkinDataLogger.isLoggable(Level.FINEST)) {
      zipkinDataLogger.info("Inbound Zipkin span: " + zipkinSpan.toString());
    }
    // Add application tags, span references , span kind and http uri, responses etc.
    List<Annotation> annotations = addAnnotations(zipkinSpan);

    /** Add source of the span following the below:
     *    1. If "source" is provided by span tags , use it else
     *    2. Set "source" to local service endpoint's ipv4 address, else
     *    3. Default "source" to "zipkin".
     */
    String sourceName = DEFAULT_SOURCE;
    if (zipkinSpan.tags() != null && zipkinSpan.tags().size() > 0) {
      if (zipkinSpan.tags().get(SOURCE_KEY) != null) {
        sourceName = zipkinSpan.tags().get(SOURCE_KEY);
      } else if (zipkinSpan.localEndpoint() != null && zipkinSpan.localEndpoint().ipv4() != null) {
        sourceName = zipkinSpan.localEndpoint().ipv4();
      }
    }
    // Set spanName.
    String spanName = zipkinSpan.name() == null ? DEFAULT_SPAN_NAME : zipkinSpan.name();

    //Build wavefront span
    Span newSpan = Span.newBuilder().
        setCustomer("dummy").
        setName(spanName).
        setSource(sourceName).
        setSpanId(Utils.convertToUuidString(zipkinSpan.id())).
        setTraceId(Utils.convertToUuidString(zipkinSpan.traceId())).
        setStartMillis(zipkinSpan.timestampAsLong() / 1000).
        setDuration(zipkinSpan.durationAsLong() / 1000).
        setAnnotations(annotations).
        build();

    // Log Zipkin spans as well as Wavefront spans for debugging purposes.
    if (zipkinDataLogger.isLoggable(Level.FINEST)) {
      zipkinDataLogger.info("Converted Wavefront span: " + newSpan.toString());
    }

    if (preprocessor != null) {
      preprocessor.forSpan().transform(newSpan);
      if (!preprocessor.forSpan().filter((newSpan))) {
        if (preprocessor.forSpan().getLastFilterResult() != null) {
          handler.reject(newSpan, preprocessor.forSpan().getLastFilterResult());
        } else {
          handler.block(newSpan);
        }
        return;
      }
    }
    handler.report(newSpan);
  }

  private List<Annotation> addAnnotations(zipkin2.Span zipkinSpan) {
    List<Annotation> annotations = new ArrayList<>();

    // Set Span's References.
    if (zipkinSpan.parentId() != null) {
      annotations.add(new Annotation(TraceConstants.PARENT_KEY,
          Utils.convertToUuidString(zipkinSpan.parentId())));
    }

    // Set Span Kind.
    if (zipkinSpan.kind() != null) {
      annotations.add(new Annotation("span.kind", zipkinSpan.kind().toString().toLowerCase()));
    }

    // Set Span's service name.
    String serviceName = zipkinSpan.localServiceName() == null ? DEFAULT_SERVICE :
        zipkinSpan.localServiceName();
    annotations.add(new Annotation(SERVICE_TAG_KEY, serviceName));

    // Set Span's Application Tag.
    // Mandatory tags are com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY and
    // com.wavefront.sdk.common.Constants.SOURCE_KEY for which we declare defaults.
    addTagWithKey(zipkinSpan, annotations, APPLICATION_TAG_KEY, DEFAULT_APPLICATION);

    // Set all other Span Tags.
    addSpanTags(zipkinSpan, annotations, ImmutableList.of(APPLICATION_TAG_KEY, SOURCE_KEY));
    return annotations;
  }

  private static void addSpanTags(zipkin2.Span zipkinSpan,
                                  List<Annotation> annotations,
                                  List<String> ignoreKeys) {
    if (zipkinSpan.tags() != null && zipkinSpan.tags().size() > 0) {
      for (Map.Entry<String, String> tag : zipkinSpan.tags().entrySet()) {
        if (!ignoreKeys.contains(tag.getKey().toLowerCase())) {
          annotations.add(new Annotation(tag.getKey(), tag.getValue()));
        }
      }
    }
  }

  private static void addTagWithKey(zipkin2.Span zipkinSpan,
                                    List<Annotation> annotations,
                                    String key,
                                    String defaultValue) {
    if (zipkinSpan.tags() != null && zipkinSpan.tags().size() > 0 && zipkinSpan.tags().get(key) != null) {
      annotations.add(new Annotation(key, zipkinSpan.tags().get(key)));
    } else if (defaultValue != null) {
      annotations.add(new Annotation(key, defaultValue));
    }
  }

  @Override
  protected void processLine(final ChannelHandlerContext ctx, final String message) {
    throw new UnsupportedOperationException("Invalid context for processLine");
  }
}

