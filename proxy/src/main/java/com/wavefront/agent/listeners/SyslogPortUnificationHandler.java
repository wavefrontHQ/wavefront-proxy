package com.wavefront.agent.listeners;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.ReportableEntityDecoder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import java.net.URISyntaxException;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;
import wavefront.report.ReportLog;

public class SyslogPortUnificationHandler extends AbstractPortUnificationHandler
    implements ChannelHandler {
  private static final Logger logger =
      Logger.getLogger(SyslogPortUnificationHandler.class.getCanonicalName());
  private final ReportableEntityHandler<ReportLog, ReportLog> handler;
  private final ReportableEntityDecoder<String, ReportLog> decoder;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;

  public SyslogPortUnificationHandler(
      String port,
      TokenAuthenticator tokenAuthenticator,
      ReportableEntityDecoder<String, ReportLog> decoder,
      ReportableEntityHandlerFactory handlerFactory,
      Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      HealthCheckManager healthCheckManager) {
    super(tokenAuthenticator, healthCheckManager, port);
    this.preprocessorSupplier = preprocessorSupplier;
    this.handler = handlerFactory.getHandler(ReportableEntityType.LOGS, port);
    this.decoder = decoder;
  }

  @Override
  protected void handleHttpMessage(ChannelHandlerContext ctx, FullHttpRequest request)
      throws URISyntaxException {
    //        logsDiscarded.get().inc();
    logger.warning("Input discarded: http protocol is not supported on port " + handle);
  }

  @Override
  protected void handlePlainTextMessage(ChannelHandlerContext ctx, @NotNull String message) {
    WavefrontPortUnificationHandler.preprocessAndHandleLog(
        message, decoder, handler, preprocessorSupplier, ctx);
  }
}
