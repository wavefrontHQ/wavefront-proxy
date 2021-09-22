package com.wavefront.agent.listeners;

import com.google.protobuf.InvalidProtocolBufferException;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.data.ReportableEntityType;

import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;
import java.util.logging.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import wavefront.report.Span;

import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;

public class OtlpHttpHandler extends AbstractHttpOnlyHandler {
  private ReportableEntityHandler<Span, String> spanHandler;
  private final static Logger logger = Logger.getLogger(OtlpHttpHandler.class.getCanonicalName());

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
                         @Nullable String handle) {
    this(tokenAuthenticator, healthCheckManager, handle);
    this.spanHandler = handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle));
  }

  @Override
  protected void handleHttpMessage(ChannelHandlerContext ctx, FullHttpRequest request) throws URISyntaxException {
    try {
      ExportTraceServiceRequest otlpRequest =
          ExportTraceServiceRequest.parseFrom(request.content().nioBuffer());
      logger.info("otlp http request: " + otlpRequest);

      for (wavefront.report.Span wfSpan :
          OtlpUtils.otlpSpanExportRequestParseToWFSpan(otlpRequest)) {
        this.spanHandler.report(wfSpan);
      }
      /*
      We use HTTP 200 for success and HTTP 400 for errors, mirroring what we found in
      OTel Collector's OTLP Receiver code.
     */
      HttpResponseStatus status = HttpResponseStatus.OK;
      StringBuilder output = new StringBuilder();
      writeHttpResponse(ctx, status, output, request);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, new StringBuilder("FAILED"), request);
  }
}
