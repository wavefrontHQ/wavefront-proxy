package com.wavefront.agent.listeners.otlp;

import com.google.protobuf.InvalidProtocolBufferException;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.listeners.AbstractHttpOnlyHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.sdk.common.WavefrontSender;

import java.net.URISyntaxException;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import wavefront.report.Span;

import static com.wavefront.agent.channel.ChannelUtils.errorMessageWithRootCause;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;

public class OtlpHttpHandler extends AbstractHttpOnlyHandler {
  private final static Logger logger = Logger.getLogger(OtlpHttpHandler.class.getCanonicalName());
  private ReportableEntityHandler<Span, String> spanHandler;

  @Nullable
  private WavefrontSender sender;
  private Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private SpanSampler sampler;

  /**
   * Create new instance.
   *
   * @param tokenAuthenticator {@link TokenAuthenticator} for incoming requests.
   * @param healthCheckManager shared health check endpoint handler.
   * @param handle             handle/port number.
   */
  public OtlpHttpHandler(@Nullable TokenAuthenticator tokenAuthenticator,
                         @Nullable HealthCheckManager healthCheckManager,
                         @Nullable String handle) {
    super(tokenAuthenticator, healthCheckManager, handle);
  }

  public OtlpHttpHandler(ReportableEntityHandlerFactory handlerFactory,
                         @Nullable TokenAuthenticator tokenAuthenticator,
                         @Nullable HealthCheckManager healthCheckManager,
                         @Nullable String handle,
                         @Nullable WavefrontSender wfSender,
                         @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                         SpanSampler sampler
                         ) {
    this(tokenAuthenticator, healthCheckManager, handle);
    this.spanHandler = handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle));
    this.sender = wfSender;
    this.preprocessorSupplier = preprocessorSupplier;
    this.sampler = sampler;
  }

  @Override
  protected void handleHttpMessage(ChannelHandlerContext ctx, FullHttpRequest request) throws URISyntaxException {
//  TODO:  if request.path == "/v1/traces"
//        else if request.p[ath == "/v1/metrics"
//        eslse blow up
    try {
      ExportTraceServiceRequest otlpRequest =
          ExportTraceServiceRequest.parseFrom(request.content().nioBuffer());
      logger.info("otlp http request: " + otlpRequest);
      OtlpProtobufUtils.exportToWavefront(otlpRequest, spanHandler, preprocessorSupplier);
      /*
      We use HTTP 200 for success and HTTP 400 for errors, mirroring what we found in
      OTel Collector's OTLP Receiver code.
     */
      writeHttpResponse(ctx, HttpResponseStatus.OK, "", request);
    } catch (InvalidProtocolBufferException e) {
      logWarning("WF-300: Failed to handle incoming OTLP request", e, ctx);
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, errorMessageWithRootCause(e), request);
    }
  }
}
