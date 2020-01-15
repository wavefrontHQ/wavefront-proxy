package com.wavefront.agent.listeners;

import com.google.common.base.Throwables;

import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.ingester.GraphiteDecoder;
import com.wavefront.ingester.ReportPointSerializer;
import com.wavefront.ingester.ReportableEntityDecoder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import wavefront.report.ReportPoint;

/**
 * Channel handler for byte array data.
 * @author Mike McLaughlin (mike@wavefront.com)
 */
@ChannelHandler.Sharable
public class ChannelByteArrayHandler extends SimpleChannelInboundHandler<byte[]> {
  private static final Logger logger = Logger.getLogger(
      ChannelByteArrayHandler.class.getCanonicalName());

  private final ReportableEntityDecoder<byte[], ReportPoint> decoder;
  private final ReportableEntityHandler<ReportPoint, String> pointHandler;

  @Nullable
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final Logger blockedItemsLogger;
  private final GraphiteDecoder recoder;

  /**
   * Constructor.
   */
  public ChannelByteArrayHandler(
      final ReportableEntityDecoder<byte[], ReportPoint> decoder,
      final ReportableEntityHandler<ReportPoint, String> pointHandler,
      @Nullable final Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      final Logger blockedItemsLogger) {
    this.decoder = decoder;
    this.pointHandler = pointHandler;
    this.preprocessorSupplier = preprocessorSupplier;
    this.blockedItemsLogger = blockedItemsLogger;
    this.recoder = new GraphiteDecoder(Collections.emptyList());
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, byte[] msg) {
    // ignore empty lines.
    if (msg == null || msg.length == 0) {
      return;
    }

    ReportableEntityPreprocessor preprocessor = preprocessorSupplier == null ?
        null : preprocessorSupplier.get();

    List<ReportPoint> points = new ArrayList<>(1);
    try {
      decoder.decode(msg, points, "dummy");
      for (ReportPoint point : points) {
        if (preprocessor != null && !preprocessor.forPointLine().getTransformers().isEmpty()) {
          String pointLine = ReportPointSerializer.pointToString(point);
          pointLine = preprocessor.forPointLine().transform(pointLine);
          List<ReportPoint> parsedPoints = new ArrayList<>(1);
          recoder.decodeReportPoints(pointLine, parsedPoints, "dummy");
          parsedPoints.forEach(x -> preprocessAndReportPoint(x, preprocessor));
        } else {
          preprocessAndReportPoint(point, preprocessor);
        }
      }
    } catch (final Exception e) {
      final Throwable rootCause = Throwables.getRootCause(e);
      String errMsg = "WF-300 Cannot parse: \"" +
          "\", reason: \"" + e.getMessage() + "\"";
      if (rootCause != null && rootCause.getMessage() != null) {
        errMsg = errMsg + ", root cause: \"" + rootCause.getMessage() + "\"";
      }
      InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
      if (remoteAddress != null) {
        errMsg += "; remote: " + remoteAddress.getHostString();
      }
      logger.log(Level.WARNING, errMsg, e);
      pointHandler.block(null, errMsg);
    }
  }

  private void preprocessAndReportPoint(ReportPoint point,
                                        ReportableEntityPreprocessor preprocessor) {
    String[] messageHolder = new String[1];
    if (preprocessor == null) {
      pointHandler.report(point);
      return;
    }
    // backwards compatibility: apply "pointLine" rules to metric name
    if (!preprocessor.forPointLine().filter(point.getMetric(), messageHolder)) {
      if (messageHolder[0] != null) {
        blockedItemsLogger.warning(ReportPointSerializer.pointToString(point));
      } else {
        blockedItemsLogger.info(ReportPointSerializer.pointToString(point));
      }
      pointHandler.block(point, messageHolder[0]);
      return;
    }
    preprocessor.forReportPoint().transform(point);
    if (!preprocessor.forReportPoint().filter(point, messageHolder)) {
      if (messageHolder[0] != null) {
        blockedItemsLogger.warning(ReportPointSerializer.pointToString(point));
      } else {
        blockedItemsLogger.info(ReportPointSerializer.pointToString(point));
      }
      pointHandler.block(point, messageHolder[0]);
      return;
    }
    pointHandler.report(point);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (cause.getMessage().contains("Connection reset by peer")) {
      // These errors are caused by the client and are safe to ignore
      return;
    }
    final Throwable rootCause = Throwables.getRootCause(cause);
    String message = "WF-301 Error while receiving data, reason: \""
        + cause.getMessage() + "\"";
    if (rootCause != null && rootCause.getMessage() != null) {
      message += ", root cause: \"" + rootCause.getMessage() + "\"";
    }
    InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
    if (remoteAddress != null) {
      message += "; remote: " + remoteAddress.getHostString();
    }
    logger.warning(message);
  }
}
